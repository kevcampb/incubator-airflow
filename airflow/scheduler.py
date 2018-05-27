#
# Reimplementation of airflow scheduling
#
# This is a work in progress reimplementation of the scheduler
#

import time
import pendulum
from datetime import datetime, timedelta
from collections import defaultdict

from airflow import models
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.utils.helpers import chunks
from airflow.utils import timezone
from airflow.settings import Stats
from airflow import executors, settings, configuration
from airflow.jobs import LocalTaskJob

from sqlalchemy.orm import aliased, make_transient
from sqlalchemy.sql.expression import func
from sqlalchemy import and_, or_, tuple_



class Scheduler(LoggingMixin):

    # Currently added to prevent logspam. Will affect throughput for loadtests though.
    MIN_LOOP_DURATION = 10

    def __init__(self, executor=executors.GetDefaultExecutor()):

        super(Scheduler, self).__init__()

        self.dagbag = None
        self.dags = {}
        self.dag_paused_status = {}
        self.executor = executor
        self._shutdown = False

    def run(self):

        session = settings.Session()

        # Scheduler has restarted, clear all tasks which are in state RUNNING as those tasks will
        # will not have completed. There's a few edge cases around here, and it's more complicated
        # when using CeleryExecutor, as those tasks may be currently in progress. For now we're 
        # assuming that tasks are idempotent and well written so that this wouldn't be a problem.
        self.reset_scheduler(session)

        session.commit()
        session.expire_all()

        # Start the executor. This will actually run the code for the task instances, either locally
        # or by queueing them through celery
        self.executor.start()

        while not self._shutdown:

            st = time.time()

            session = settings.Session()

            # Reload the dag files from disk if they are modified
            self.reload_dags(session)

            # The reload dags method currently invalidates the session, get a new one
            session.commit()
            session = settings.Session()

            # Check database for anomalies (temporary fix during new scheduler migration)
            self.check_anomalies(session)
        
            # Check responses from the executor
            self.process_executor_replies(session)

            # Retry tasks which have failed
            # self.retry_failed_tasks(session)

            # If any task is in state running but has exceeded timeout, set state to failed
            self.handle_timed_out_tasks(session)

            # Run dependency checks. At present this returns back a list of tasks_to_run. As a 
            # side effect updates task state to SKIPPED / UPSTREAM_FAILED
            tasks_to_run = self.run_dependency_checks(session)

            # Enqueue tasks to workers, respecting concurrency limits and task dependencies
            active_task_keys = self.queue_tasks_to_workers(session, tasks_to_run)

            # Check to see if we need to create new DagRuns and TaskInstances for Dags which run
            # on a schedule. We do this after the dependency checks so we know how many tasks
            # are ready to run. We only create new dagruns if the dag concurrency is not reached
            self.build_dagruns_and_tasks(session, active_task_keys)
            
            # Check SLAs and alert if any tasks haven't been completed on time
            self.perform_sla_check(session)

            # Now run the executor. Some executors will update state in the DB
            self.log.info("Heartbeating executor")
            self.executor.heartbeat()

            # Finally, check the state of any DagRun objects which are 'RUNNING'
            self.update_active_dagrun_states(session)

            # Flush all state to the database. This will clear any cached database objects, so the 
            # next iteration reloads from the database.
            session.commit()

            et = time.time()
            self.log.info("Scheduler loop complete in %.02f seconds" % (et - st))

            if et - st < self.MIN_LOOP_DURATION:
                sleep_time = self.MIN_LOOP_DURATION - (et - st)
                self.log.info("Sleeping for %.02f seconds" % sleep_time)
                time.sleep(sleep_time)

        self.log.info("Shutting down executor")
        # Wait for executor to exit and shutdown cleanly
        self.executor.end()

        # Process final replies as tasks may still have been running as we shut down
        self.process_executor_replies(session)

    def check_anomalies(self, session):
        """ Temporary fixes for a few conditions which may happen on migration
            to new scheduler. This section should be removed and changed into some 
            form of database migration, or larger scale fixes of the codebase
        """
        # Check for TaskInstances which do not have associated DagRuns
        tis = (session.query(models.TaskInstance)
                .outerjoin(models.DagRun, and_(
                    models.TaskInstance.dag_id == models.DagRun.dag_id,
                    models.TaskInstance.execution_date == models.DagRun.execution_date,
                ))
                .filter(models.DagRun.execution_date == None)
              )
        
        for ti in tis:  
            self.log.warning("Task Instance {} {} {} has no DagRun, removing".format(ti.dag_id, ti.task_id, ti.execution_date))
            session.delete(ti)
        session.commit()

    def process_executor_replies(self, session):
        responses = self.executor.get_event_buffer(self.dags.keys())

        dag_runs_to_check = set()
        for task_key, state in responses.items():
            (dag_id, task_id, execution_date) = task_key 
            self.log.info(
                "Executor reports %s %s %s as %s",
                dag_id, task_id, execution_date, state
            )

            dag_runs_to_check.add((dag_id, execution_date))

        # Now for all affected dags, we should recheck their status and set the state accordingly
        # TODO: implement this


    def reload_dags(self, session):
        self.dagbag = models.DagBag()
        self.dags = {}
        
        # We are forced to do this for now, to avoid having to change the dagbag code
        # calling get_dag resets the sqlalchemy session
        for dag_id in self.dagbag.dags:
            dag = self.dagbag.get_dag(dag_id)
            self.dags[dag_id] = dag

            # Update the database. 
            dag.sync_to_db()

            # This is maddening. The Dag object in the codebase actually does a db query when you read the 
            # property is_paused. Additionally, it resets the sqlalchemy session. For now as a workaround
            # we store this value for all dags on reload
            self.dag_paused_status[dag_id] = dag.is_paused

        dagbag_dag_task_values = set([])
        for dag_id, dag in self.dags.items():
            for task_id in dag.task_ids: 
                dagbag_dag_task_values.add((dag_id, task_id)) 

        # Check for any TaskInstances which have a dag_run, task_id which is not in the dag list
        # We will set the state of these to REMOVED for now
        session = settings.Session()

        dag_task_values = (session.query(models.TaskInstance.dag_id, models.TaskInstance.task_id)
            .filter(or_(
                models.TaskInstance.state != State.REMOVED,
                models.TaskInstance.state == None
            ))).distinct()

        for (dag_id, task_id) in dag_task_values:
            if (dag_id, task_id) not in dagbag_dag_task_values:

                tis = (session.query(models.TaskInstance)
                    .filter(
                        and_(
                            models.TaskInstance.dag_id == dag_id,
                            models.TaskInstance.task_id == task_id,
                            or_(    
                                models.TaskInstance.state != State.REMOVED,
                                models.TaskInstance.state == None
                            )
                        )))

                self.log.warn("Task %(dag_id)s %(task_id)s no longer exists in Dag files on disk. Settings %(count)i task instances to state REMOVED" % {
                    'dag_id': dag_id,
                    'task_id': task_id,
                    'count': tis.count(),
                })
            
                tis.update({'state': State.REMOVED})

        # Check for any TaskInstance which was set to REMOVED and check it hasn't been reinstated
        dag_task_values = (session.query(models.TaskInstance.dag_id, models.TaskInstance.task_id)
            .filter(models.TaskInstance.state == State.REMOVED)
        ).distinct()

        for (dag_id, task_id) in dag_task_values:
            if (dag_id, task_id) in dagbag_dag_task_values:

                tis = (session.query(models.TaskInstance)
                    .filter(
                        and_(
                            models.TaskInstance.dag_id == dag_id,
                            models.TaskInstance.task_id == task_id,
                            models.TaskInstance.state == State.REMOVED
                        )))

                self.log.warn("Task %(dag_id)s %(task_id)s has been restored on disk. Re-activating %(count)i task instances which were in the REMOVED state" % {
                    'dag_id': dag_id,
                    'task_id': task_id,
                    'count': tis.count(),
                })
            
                tis.update({'state': State.NONE})
            
    def dag_is_paused(self, dag_id):
        return self.dag_paused_status[dag_id]

    def reset_scheduler(self, session):
        """ Reset the database state when the scheduler is restarted
        
            Tasks which are in the queued state need to basck to None as the executor queue will now have
            been reset. Those tasks will get their dependencies checked again, and re-raised as expected
        """

        LOCAL_EXECUTORS = (
            executors.LocalExecutor, 
            executors.SequentialExecutor,
#            executors.Executors.DummyExecutor, 
        )

        tis = (session.query(models.TaskInstance)
                .filter(or_(
                    models.TaskInstance.state == State.SCHEDULED,
                    models.TaskInstance.state == State.QUEUED,
                )))

        self.log.info("Resetting state for {} queued task instances due to scheduler restart".format(tis.count()))

        tis.update({'state': State.NONE})

        tis = (session.query(models.TaskInstance)
            .filter(models.TaskInstance.state==State.RUNNING)
        )
    
        if isinstance(self.executor, LOCAL_EXECUTORS):
            self.log.info("Resetting state for {} running task instances due to scheduler restart".format(tis.count()))
            tis.update({'state': State.NONE})
        else:
            if tis.count() > 1:
                self.log.warn("Tasks are currently in the RUNNING state and you are running a distributed executor. You need to wait for these tasks to time out")
                time.sleep(2)
            else:
                self.log.info("No tasks in the RUNNING state on scheduler restart")

    def update_active_dagrun_states(self, session):

        # DagRun state is a derived state from whether all tasks are active or not
        #   All task instances are in state SUCCESS -> DagRun is in state SUCCESS
        #   All task instances are in state FAILURE -> DagRun is in state FAILURE
        #   DagRun is in state RUNNING
        #
        # Example of fixing this using SQL
        # TODO: Still thinking about this        

        QUERY = """
            UPDATE dag_run 
                SET state = expected_state 
                FROM (
                  SELECT dag_id, execution_date, dr_state,
                      CASE
                          WHEN success_count = total_count THEN 'success'
                          WHEN failed_count = total_count THEN 'failure'
                          ELSE 'running'
                      END AS expected_state
                  FROM (
                    SELECT

                      dag_id, execution_date, dr_state,
                      SUM(CASE WHEN ti_state = 'success' OR ti_state = 'skipped' THEN 1 ELSE 0 END) AS success_count,
                      SUM(CASE WHEN ti_state = 'failed' OR ti_state = 'upstream_failed' THEN 1 ELSE 0 END) AS failed_count,
                      COUNT(dr_state) AS total_count

                      FROM (
                        SELECT
                          dag_run.dag_id, dag_run.execution_date,
                          dag_run.state AS dr_state,
                          task_instance.state AS ti_state
                        FROM dag_run
                          LEFT JOIN task_instance
                            ON task_instance.execution_date = dag_run.execution_date
                              AND task_instance.dag_id = dag_run.dag_id
                      ) AS dag_run_task_states
                      GROUP BY dag_id, execution_date, dr_state

                  ) AS dag_run_state_counts

                ) AS dag_run_derived_states

               WHERE dag_run.dag_id = dag_run_derived_states.dag_id 
                    AND dag_run.execution_date = dag_run_derived_states.execution_date
                    AND dr_state != expected_state
        """

        session.execute(QUERY)
        

    def handle_timed_out_tasks(self, session):

        self.log.info("Finding 'running' jobs without a recent heartbeat")
        secs = configuration.conf.getint('scheduler', 'scheduler_zombie_task_threshold')

        now = timezone.utcnow()
        limit_dttm = now - timedelta(seconds=secs)
        self.log.info("Failing jobs without heartbeat after %s", limit_dttm)

        # Handle RUNNING tasks without heartbeat

        tis = (
            session.query(models.TaskInstance, LocalTaskJob)
            .outerjoin(LocalTaskJob, models.TaskInstance.job_id == LocalTaskJob.id)
            .filter(
                models.TaskInstance.state == State.RUNNING
            )
            .filter(
                or_(
                    LocalTaskJob.state == None,
                    LocalTaskJob.state != State.RUNNING,
                    LocalTaskJob.latest_heartbeat < limit_dttm,
                )
            )
        )

        for ti, ltj in tis:

            if ltj is None:
                self.log.info("TaskInstance {} {} {} is in state {} but has no matching Job in the database, resetting".format(ti.dag_id, ti.task_id, ti.execution_date, ti.state))
            elif ltj.state != State.RUNNING:
                self.log.info("TaskInstance {} {} {} is in state {} but Job is in state {}, resetting".format(ti.dag_id, ti.task_id, ti.execution_date, ti.state, ltj.state))
            elif ltj.latest_heartbeat < limit_dttm:
                delta = pendulum.instance(now) - pendulum.instance(ltj.latest_heartbeat)
                self.log.info("TaskInstance {} {} {} is in state {} but the Job's last heartbeat is {} ago".format(ti.dag_id, ti.task_id, ti.execution_date, ti.state, delta))
            else:
                raise LogicError("This condition should not be reached")

            dag = self.dags[ti.dag_id]
            task = dag.get_task(ti.task_id)

            error = "{} killed as zombie".format(str(ti))

            self.log.info('Marked zombie job %s as failed', ti)
            Stats.incr('zombies_killed')

            ti.task = task

            new_state = self.handle_failed_task(session, ti, error)

            ti.state = new_state

        # Handle QUEUED tasks which the executor does not know about
        
        tis = (
            session.query(models.TaskInstance)
            .filter(models.TaskInstance.state == State.QUEUED)
        )
        
        for ti in tis:
            if ti.key not in self.executor.queued_tasks and ti.key not in self.executor.running:
                self.log.info("TaskInstance {} {} {} disappeared after it was sent to the executor, resetting state".format(ti.dag_id, ti.task_id, ti.execution_date))
                ti.state = State.NONE

        # Commit to DB now as emails have been sent
        session.commit()


    def handle_failed_task(self, session, ti, error):
        """ We call this on task when either:
              The executor replies the task has failed
              We decide the task is a zombie as there has been no heartbeat

            Returns
              (new_state, email)
        """

        ti.end_date = timezone.utcnow()
        ti.set_duration()

        Stats.incr('operator_failures_{}'.format(ti.task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')

        session.add(models.Log(State.FAILED, ti))
        session.add(models.TaskFail(ti.task, ti.execution_date, ti.start_date, ti.end_date))

        if ti.task.retries and ti.try_number <= ti.max_tries:
            new_state = State.UP_FOR_RETRY
            self.log.info('Marking task as UP_FOR_RETRY')
            if ti.task.email_on_retry and ti.task.email:
                try:
                    ti.email_alert(error, is_retry=True)
                except Exception as e2:
                    self.log.error('Failed to send email to: %s', ti.task.email)
                    self.log.exception(e2)

        else:
            new_state = State.FAILED
            if ti.task.retries:
                self.log.info('All retries failed; marking task as FAILED')
            else:
                self.log.info('Marking task as FAILED.')
            if ti.task.email_on_failure and ti.task.email:
                try:
                    ti.email_alert(error, is_retry=True)
                except Exception as e2:
                    self.log.error('Failed to send email to: %s', ti.task.email)
                    self.log.exception(e2)

        self.log.error(str(error))

        return new_state


    def perform_sla_check(self, session):
        # TODO: Implement this
        pass
        

    def get_active_task_key_states(self, session):
        """ Returns a set of (dag_id, task_id, execution_date, state) for all active tasks """

        res = (session.query(models.TaskInstance.dag_id, models.TaskInstance.task_id, models.TaskInstance.execution_date, models.TaskInstance.state)
            .filter(models.TaskInstance.state.in_((State.RUNNING, State.QUEUED)))
        )
    
        return set([
            (ti.dag_id, ti.task_id, ti.execution_date, ti.state) for ti in res
        ])


    def queue_tasks_to_workers(self, session, tasks_to_run):

        # Count the number of active tasks in the database
        #
        # The true value may decrease in the background as we go through this method
        #   executor will set some tasks from running to success / failed
        #   current implementation of ShortCircuitOperator 
        #
        # If it decreases all that happesn is we queue less task this loop than we'd like, which is fine
        #
        # It may also increase with the current implementation of SubDagOperator and TriggerDagRunOperator
        # but lets ignore this for now. Those tasks will likely end up failing and getting rescheduled.
        # This is probably what happened in the old scheduler as well anyway, although the race condition
        # may have been tighter

        # Get a count of active tasks
        # We want the following structures
        #   active_dag_runs = {dag_id: set(exection_dates)}
        #   active_task_instances = {(dag_id, task_id): set(execution_dates)}
        st = time.time()
        active_task_key_states = self.get_active_task_key_states(session)

        self.log.info("Currently %i active TaskInstances according to database" % len(active_task_key_states))

        active_dag_runs = defaultdict(set)
        active_dag_tasks = defaultdict(set)
        active_task_instances = defaultdict(set)
        for dag_id, task_id, execution_date, state in active_task_key_states:
            active_dag_runs[dag_id].add((execution_date))
            active_dag_tasks[dag_id].add((task_id, execution_date))
            active_task_instances[(dag_id, task_id)].add((execution_date))
            self.log.info("  TaskInstance {} {} {} is in state {}".format(dag_id, task_id, execution_date, state))

        et = time.time()
        self.log.info("Loaded active task counts in %.02f seconds" % (et - st))

        execution_queue = []

        for ti in tasks_to_run:

            dag = self.dags[ti.dag_id]
            task = dag.get_task(ti.task_id)

            # Don't queue tasks for paused tasks
            if self.dag_is_paused(ti.dag_id):
                continue

            # Ensure that we respect the max concurrency for the Dag
            # Max DagRuns per Dag
            if len(active_dag_runs[ti.dag_id]) >= dag.max_active_runs:
                continue
            # Max TaskInstances per Dag
            if len(active_dag_tasks[ti.dag_id]) >= dag.concurrency:
                continue

            # Ensure that we respect the max concurrency for the Task
            if task.task_concurrency:
                if len(active_task_instances[(ti.dag_id, ti.task_id)]) >= task.task_concurrency:
                    continue

            # Check that the executor does not think it is running the task
            # There is a design issue where the database state and celery replies are not
            # in sync. We skip any task instances which may be affected by this, and they
            # should get picked up on a subsequent scheduler pass
            if ti.key in self.executor.queued_tasks or ti.key in self.executor.running:            
                continue

            command = " ".join(
                models.TaskInstance.generate_command(
                    ti.dag_id,
                    ti.task_id,
                    ti.execution_date,
                    local = True,
                    mark_success = False,
                    ignore_all_deps = False,
                    ignore_depends_on_past = False,
                    ignore_task_deps = False,
                    ignore_ti_state = False,
                    pool = ti.pool,
                    file_path = dag.full_filepath,
                    pickle_id = dag.pickle_id
                )
            )

            priority = ti.priority_weight
            queue = ti.queue

            self.log.info(
                "Sending {} {} {} to executor with priority {} and queue {}, current state is {}".format(ti.dag_id, ti.task_id, ti.execution_date, priority, queue, ti.state)
            )

            ti.state = State.QUEUED
            ti.queued_dttm = timezone.utcnow()
        
            # Maintain our counts 
            active_task_key_states.add((ti.dag_id, ti.task_id, ti.execution_date, ti.state))
            active_dag_tasks[ti.dag_id].add((ti.task_id, ti.execution_date))
            active_dag_runs[ti.dag_id].add(ti.execution_date)
            active_task_instances[ti.task_id].add(ti.execution_date)

            # Record that we well send this to the executor
            execution_queue.append((ti, command, priority, queue))

        # Commit before sending to the executor. We need to do this at present as the worker processes will check and modify
        # task state. If we don't commit before sending to the queue there is a potential race condition.

        session.commit()

        for (ti, command, priority, queue) in execution_queue:

            # The executor will attempt to reload the task later, detach it from the session to prevent an exception
            # TODO: Remove this if we refactor the executor, this check should be unnecessary

            # save attributes so sqlalchemy doesnt expire them
            copy_dag_id = ti.dag_id
            copy_task_id = ti.task_id
            copy_execution_date = ti.execution_date
            make_transient(ti)
            ti.dag_id = copy_dag_id
            ti.task_id = copy_task_id
            ti.execution_date = copy_execution_date

            self.executor.queue_command(
                ti,
                command,
                priority = priority,
                queue = queue
            )

        task_states = [state for (_,_,_,state) in active_task_key_states]

        self.log.info("Executor now has %i queued / %i running TaskInstances" % (
            sum([state == State.QUEUED for state in task_states]),
            sum([state == State.RUNNING for state in task_states])
        ))

        # Return our active task list. We will use them in the next processing stage and this saves pulling them
        # from the database again

        return [ (dag_id, task_id, execution_date) 
            for (dag_id, task_id, execution_date, state) in active_task_key_states
        ]
        

    def run_dependency_checks(self, session):

        st_tot = time.time()

        # Find the task instances we want to run depdency checks for. 
        # We retain this a subquery as we will pass it to the inner methods which pull in the 
        # additional data for each task instance, on order to carry out dependencies

        ti_query = (session.query(models.TaskInstance)
                     .filter(or_(
                          models.TaskInstance.state == State.NONE,
                          models.TaskInstance.state == State.UP_FOR_RETRY
                   )))

        task_instances = list(ti_query)
        task_subquery = ti_query.subquery()

        self.log.info("Analysing dependencies for %i task instances" % len(task_instances))

        # Find the state of all previous task instances
        # This gets a maping in the form
        #   (dag_id, task_id, execution_date): previous_task_instance_state

        # TODO: This could be filtered by task instances where the task has depends_on_past = True
        # which would reduce the amount of data loaded from the database

        previous_task_state_map = self._get_previous_task_states(session, task_subquery)
 
        # Find the state of all other task instances in the same dag, for each task 
        # This returns (dag_id, execution_date) -> {task_id: state, task_id: state, ...} 
        #   for all tasks in the dag runs in the task instance list

        # TODO: This could be filtered by task instances where there are dependencies on other tasks

        dr_ti_state_map = self._get_related_ti_states(session, task_subquery)
        
        # Now process each task instance in turn
        
        tasks_to_queue = []
        for ti in task_instances:

            dag = self.dags[ti.dag_id]
            task = dag.get_task(ti.task_id)

            if self.dag_is_paused(ti.dag_id):
                # Don't do dependency checks on paused dags
                continue

            # Get the previous task instance state and related task instance states for this task
            try:
                prev_task_date, prev_task_state = previous_task_state_map[(ti.dag_id, ti.task_id, ti.execution_date)]
                dag_task_states = dr_ti_state_map[(ti.dag_id, ti.execution_date)]
            except Exception as e:
                # We may get an exception here, in two known scenarios:
                #  - The scheduler was restart whilst a TaskInstance was in the executor queue (certain executors only)
                #  - Someone deleted a TaskInstance or DagRun from the UI
                # This may result in the task instance list changing midway through this method
                # We can skip processing this TaskInstance this run, it should work on the next pass
                self.log.error("Error querying the state for {} {} {} - state was modified externally".format(ti.dag_id, ti.task_id, ti.execution_date))
                continue
                
            # Nasty hack here. Because one of the possible states for a task instance is None, we cannot use None to 
            # represent there being no previous task instance
            if prev_task_date is None:
                prev_task_state = 'no_previous_task'

            (deps_passed, new_state) = self.task_instance_dependency_check(ti, task, prev_task_state, dag_task_states)

            if not deps_passed:
                if new_state is not None:
                    ti.state = new_state
            else:
                tasks_to_queue.append(ti)

        et_tot = time.time()
        self.log.info("Dependency checks complete in %.02f seconds, have %i tasks ready to execute" % (et_tot - st_tot, len(tasks_to_queue)))

        return tasks_to_queue
    

    def task_instance_dependency_check(self, ti, task, prev_task_state, dag_task_states):
        """ Run a depdendency check for a single task instance

            ti - The TaskInstance being considered
            task - The task obtained from DAG.get_task, required for us to introspect the dag structure
            prev_task_state - An instance of airflow.utils.State, current state of the same task in the previous DagRun 
                              If no previous DagRun exists, this is set to 'no_previous_task'
            dag_task_states - A mapping of {task_id: State} for all TaskInstances in the same DagRun
        
            Returns - (passed, new_state)
              passed - boolean, true if task is ready to execute
              new_state - if passed is False, the state the task should now be set to
        """

        self.log.debug("Dependency check for {} {} {}".format(ti.dag_id, ti.task_id, ti.execution_date))
        self.log.debug("Previous task instance state: %s" % (prev_task_state))
        self.log.debug("States of other tasks in the same DagRun: %s" % str(dag_task_states))

        # Note that it's possible a parent doesn't exist, if a dagrun was modified once the dag was running
        # In this case, we ignore those task states. The semantics of ALL_SUCCESS, etc still make sense with
        # missing task instances
    
        parent_task_states = set([
            dag_task_states[parent] for parent in task.upstream_task_ids
            if parent in dag_task_states
        ])

        # Check if depends_on_past is set, and if the previous task has run yet
        # If there is no previous task instance, then queue this task. This situation should only be reached
        # for the very first task instance in a dag

        if task.depends_on_past:

            if prev_task_state == 'no_previous_task':
                self.log.debug("Task has depends_on_past set, but there is no previous DagRun. Allowing task to proceed")

            elif prev_task_state in set([State.SUCCESS, State.SKIPPED]):
                self.log.debug("Task has depends_on_past set, and previous task is in state '%s'. Allowing task to proceed" % prev_task_state)
            
            elif prev_task_state == State.FAILED:
                self.log.debug("Setting task %i to state 'upstream_failed' as depends_on_past set true and previous task failed")
                return (False, State.FAILED)

            else:
                self.log.debug("Leaving task %i in state 'none' as depends_on_past set true and previous task hasn't executed")
                return (False, None)


        # Now check dependencies for the tasks in the same DagRun
        # There are some very odd semanatics here, and it's probably not exactly correct. For example,
        # when there is a SKIPPED parent task, but the trigger rule is all_success, does that qualify?
        # Currently trying to match the old semantics for compatability.

        # The definitions in airflow.utils.states.finished does not appear to be correct, so we will
        # redefine here, rather than changing that file - it will make merging easier for now

        FINAL_STATES = set([State.FAILED, State.SUCCESS, State.SKIPPED, State.UPSTREAM_FAILED])

        if task.trigger_rule == models.TriggerRule.ALL_SUCCESS:
            non_success_states = parent_task_states - set([State.SUCCESS])
            if non_success_states:
                final_non_success_states = parent_task_states & (FINAL_STATES - set([State.SUCCESS]))
                if len(final_non_success_states) > 0:
                    self.log.debug("Setting task %s to state 'upstream_failed' as the trigger rule is 'all_success' and there are upstream tasks in a failed state" % ti)
                    return (False, State.UPSTREAM_FAILED)
                else:
                    state_str = ','.join(sorted("'%s'" % s for s in non_success_states))
                    self.log.debug("Leaving task %s unqueued as the trigger rule is 'all_success' but there are tasks in states %s" % (ti, state_str))
                    return (False, None)

        if task.trigger_rule == models.TriggerRule.ALL_FAILED:
            non_failure_states = parent_task_states - set([State.FAILURE])
            if non_failure_states:
                final_non_failure_states = parent_task_states & (FINAL_STATES - set([State.FAILURE]))
                if len(final_non_failure_states) > 0:
                    self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'all_failure' and there are upstream tasks which did not fail" % ti)
                    return (False, State.SKIPPED)
                else:
                    state_str = ','.join(sorted("'%s'" % s for s in non_failure_states))
                    self.log.debug("Leaving task %s unqueued as the trigger rule is 'all_failure' but there are tasks in states %s" % (ti, state_str))
                    return (False, None)

        if task.trigger_rule == models.TriggerRule.ONE_SUCCESS:
            if State.SUCCESS not in parent_task_states:
                if len(parent_task_states - FINAL_STATES) == 0:
                    self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'one_success' and all upstream tasks are finished, but no tasks have succeeded" % ti)
                    return (False, State.SKIPPED)
                else:
                    self.log.debug("Leaving task %s unqueued as the trigger rule is 'one_success' but no tasks have succeeded" % ti)
                    return (False, None)

        if task.trigger_rule == models.TriggerRule.ONE_FAILED:
            if State.FAILURES not in parent_task_states:
                if len(parent_task_states - FINAL_STATES) == 0:
                    ti.state = State.SKIPPED
                    self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'one_failure' and all upstream tasks are finished, but no tasks have failed" % ti)
                    return (False, State.SKIPPED)
                else:
                    self.log.debug("Leaving task %s unqueued as the trigger rule is 'one_failure' but no tasks have failed" % ti)
                    return (False, None)

        return (True, None)
        

    def build_dagruns_and_tasks(self, session, active_task_keys):

        active_dag_runs = defaultdict(set)
        for dag_id, task_id, execution_date in active_task_keys:
            active_dag_runs[dag_id].add((execution_date))

        for dag_id in self.dagbag.dags:

            dag = self.dags[dag_id]

            if dag.is_subdag:
                # Don't create DagRun or TaskInstance objects for SubDags. They get created by the SubDagOperator
                # which is a bit odd, but we'll leave it for now during this refactor
                continue

            if self.dag_is_paused(dag_id):
                self.log.info("Not processing DAG %s since it's paused", dag.dag_id)
                continue
                
            self.log.info("Processing %s", dag.dag_id)

            st = time.time()

            # Find the next DagRun which should be created, following the schedule. For one off Dags, this will
            # return the current time, if no DagRun exists. If no DagRun needs to be created for this Dag, then 
            # returns None

            next_run_date = dag.get_next_run_date(
                session = session, 
                last_scheduled_run = None
            )

            # Count the number of new objects this loop - for debug log purposes only
            dr_count = 0
            ti_count = 0

            while next_run_date: 

                active_dag_run_count = len(active_dag_runs[dag_id])

                if active_dag_run_count >= dag.concurrency:
                    self.log.debug("Reached DagRun creation limit for this scheduler loop")
                    break

                # TODO: Find a way to put a lambda around this as if logging is not set to debug we are
                # calling strftime unnecessarily a lot of times
                self.log.debug("Creating new DagRun %s::%s" % (dag_id, next_run_date.strftime("%Y-%m-%dT%H:%M:%S")))

                # Create the DagRun 
                dr = models.DagRun(
                    dag_id = dag_id,
                    run_id = models.DagRun.ID_PREFIX + next_run_date.isoformat(),
                    execution_date = next_run_date,
                    start_date = timezone.utcnow(),
                    external_trigger = False,
                )
                dr._state = State.RUNNING
                session.add(dr)
                dr_count += 1

                active_dag_runs[dag_id].add(next_run_date)

                # Create the TaskInstances
                for task in dag.tasks:
                    if task.adhoc:
                        continue

                    ti = models.TaskInstance(
                        task, 
                        next_run_date,
                    )
                    ti.state = State.NONE
                    session.add(ti)
                    ti_count += 1
            
                next_run_date = dag.get_next_run_date(
                    session = session, 
                    last_scheduled_run = next_run_date
                )

            # Commit per DAG for shorter commits
            session.commit()    

            et = time.time()
            self.log.info("Created %i new DagRuns and %i TaskInstances in %.02f seconds", dr_count, ti_count, (et-st))



    def _get_previous_task_states(self, session, task_subquery):
    
        # Build up a mapping from (dag_id, task_id, execution_date) -> (prev_execution_date, previous_state)
        # so we can do the dependency check for the current tasks

        # We create a query to give us the previous execution date for any task instance. In the case that
        # there was no previous instance, we get a None value
        #
        #  dag_id    | task_id   |  execution_date     | prev_execution_date
        # -----------+-----------+---------------------+---------------------
        #  test_dag  | task-1    | 2018-04-22 00:00:00 |
        #  test_dag  | task-2    | 2018-04-22 00:00:00 |
        #  test_dag  | task-3    | 2018-04-22 00:00:00 |
        #  test_dag  | task-1    | 2018-04-23 00:00:00 | 2018-04-22 00:00:00
        #  test_dag  | task-2    | 2018-04-23 00:00:00 | 2018-04-22 00:00:00
        #  test_dag  | task-3    | 2018-04-23 00:00:00 | 2018-04-22 00:00:00

        # Join the previous table instance

        prev_task_instance = aliased(models.TaskInstance)

        prev_execution_date = (session.query(func.max(prev_task_instance.execution_date))
                                .filter(prev_task_instance.execution_date < task_subquery.c.execution_date)
                                .filter(prev_task_instance.task_id == task_subquery.c.task_id)
                                .filter(prev_task_instance.dag_id == task_subquery.c.dag_id)
                                .label("prev_execution_date"))

        ti_and_prev_tis = session.query(task_subquery.c.dag_id, task_subquery.c.task_id, task_subquery.c.execution_date, prev_execution_date)

        # Now join the previous table back to the TaskInstance table, so we can obtain the state of the 
        # previous TaskInstance

        prev = aliased(models.TaskInstance)

        ti_and_prev_tis_sq = ti_and_prev_tis.subquery()
        query = (session.query(ti_and_prev_tis_sq.c.dag_id, ti_and_prev_tis_sq.c.task_id, ti_and_prev_tis_sq.c.execution_date, prev.execution_date, prev.state)
                .outerjoin(prev,
                    and_(
                        ti_and_prev_tis_sq.c.dag_id == prev.dag_id,
                        ti_and_prev_tis_sq.c.task_id == prev.task_id,
                        ti_and_prev_tis_sq.c.prev_execution_date == prev.execution_date
                    )
                )
            )

        # Build up the mapping from (dag_id, task_id, execution_date) -> (prev_execution_date, previous_state)

        # print("Running", query)

        previous_task_state_map = {
            (dag_id, task_id, execution_date): (prev_execution_date, previous_state)
            for (dag_id, task_id, execution_date, prev_execution_date, previous_state) in query
        }
    
        return previous_task_state_map

    def _get_related_ti_states(self, session, task_subquery):
        """ Returns a mapping of (dag_id, execution_date) -> task_id -> state
              eg: ('test_dag', '2018-04-22 00:00:00') : { 'task_1': 'success', 'task_2': 'queued', 'task_3': 'failure'}

            For all DagRuns related to the query TaskInstances (query_tis)
        """

        ti_states = (session.query(models.TaskInstance.dag_id, models.TaskInstance.task_id, models.TaskInstance.execution_date, models.TaskInstance.state)
            .filter(models.TaskInstance.execution_date == models.DagRun.execution_date)
            .filter(models.TaskInstance.dag_id == models.DagRun.dag_id)
            .filter(task_subquery.c.execution_date == models.DagRun.execution_date)
            .filter(task_subquery.c.dag_id == models.DagRun.dag_id)
            .distinct())
        
        # print("Running", ti_states)

        dr_ti_state_map = defaultdict(dict)
        for (dag_id, task_id, execution_date, state) in ti_states:
            dr_ti_state_map[(dag_id, execution_date)][task_id] = state

        return dict(dr_ti_state_map)  # convert the defaultdict into a dict
         
    def shutdown(self):
        print ("Instructing scheduler to shut down")
        self.shutdown_status = True

