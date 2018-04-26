#
# Reimplementation of airflow scheduling
#
# This is a work in progress reimplementation of the scheduler
#

import time
from datetime import datetime, timedelta
from collections import defaultdict

from airflow import models
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.utils import timezone
from airflow import executors, settings

from sqlalchemy.orm import aliased, make_transient
from sqlalchemy.sql.expression import func
from sqlalchemy import and_, or_, tuple_

TASK_LIMIT = 200

class Scheduler(LoggingMixin):

    MAX_CREATE_DAGRUN_PER_LOOP = 1000

    def __init__(self, executor=executors.GetDefaultExecutor()):

        super(Scheduler, self).__init__()

        self.dagbag = None
        self.dags = {}
        self.active_tasks = set()
        self.executor = executor

        self._shutdown = False

    def run(self):

        session = settings.Session()

        # Scheduler has restarted, clear all tasks which are in state RUNNING as those tasks will
        # will not have completed. There's a few edge cases around here, and it's more complicated
        # when using CeleryExecutor, as those tasks may be currently in progress. For now we're 
        # assuming that tasks are idempotent and well written so that this wouldn't be a problem.
        self.reset_active_tasks(session)

        session.commit()
        session.expire_all()

        # Start the executor. This will actually run the code for the task instances, either locally
        # or by queueing them through celery
        self.executor.start()
    
        while not self._shutdown:

            # Reload the dag files from disk if they are modified
            self.reload_dags()

            session = settings.Session()

            # Check responses from the executor
            self.process_executor_replies(session)

            # Retry tasks which have failed
            # self.retry_failed_tasks(session)

            # If any task is in state running but has exceeded timeout, set state to failed
            self.fail_timed_out_tasks(session)

            # Check to see if we need to create new DagRuns and TaskInstances for Dags which run
            # on a schedule
            self.build_dagruns_and_tasks(session)
                
            # Run dependency checks. At present this returns back a list of tasks_to_run. As a 
            # side effect updates task state to SKIPPED / UPSTREAM_FAILED
            tasks_to_run = self.run_dependency_checks(session)
            
            # Enqueue tasks to workers, respecting concurrency limits and task dependencies
            self.queue_tasks_to_workers(session, tasks_to_run)

            # Check SLAs and alert if any tasks haven't been completed on time
            self.perform_sla_check(session)

            self.log.info("Heartbeating executor")
            self.executor.heartbeat()

            # Flush all state to the database. This will clear any cached database objects, so the 
            # next iteration reloads from the database.
            session.commit()

            time.sleep(0.5)

        self.log.info("Shutting down executor")
        # Wait for executor to exit and shutdown cleanly
        self.executor.end()

        # Process final replies as tasks may still have been running as we shut down
        self.process_executor_replies(session)


    def process_executor_replies(self, session):
        responses = self.executor.get_event_buffer(self.dags.keys())

        dag_runs_to_check = set()
        for task_key, state in responses.items():
            (dag_id, task_id, execution_date) = task_key 
            self.log.info(
                "Executor reports %s %s %s as %s",
                dag_id, task_id, execution_date, state
            )

            if task_key not in self.active_tasks:
                self.log.warn("We got a executor response for %s %s %s which we didn't seem to have queued" % (dag_id, task_id, execution_date))
                # This should not happen with local executor! It probably shouldn't happen with celery either
                # TODO: Leaving this debug line in to see if this happens again
                from IPython import embed
                embed()
            
            self.active_tasks.remove(task_key)
            dag_runs_to_check.add((dag_id, execution_date))

        # Now for all affected dags, we should recheck their status and set the state accordingly
        # TODO: implement this


    def reload_dags(self):
        self.dagbag = models.DagBag()
        self.dags = {}
        
        # We are forced to do this for now, to avoid having to change the dagbag code
        # calling get_dag resets the sqlalchemy session
        for dag_id in self.dagbag.dags:
            self.dags[dag_id] = self.dagbag.get_dag(dag_id)

    def reset_active_tasks(self, session):
        """ reset the database state 
        
            Note: It's possible some tasks are still active in an worker even though the scheduler
            has restarted. In this scenario, hmmm... annoying
        """

        tis = session.query(models.TaskInstance).filter(
            or_(models.TaskInstance.state==State.RUNNING, models.TaskInstance.state==State.QUEUED)
        )
        tis.update({'state': State.NONE})

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
                      COUNT(ti_state) AS total_count

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
        

    def fail_timed_out_tasks(self, session):
        # TODO: Implement this
        pass

    def perform_sla_check(self, session):
        # TODO: Implement this
        pass
        
    def queue_tasks_to_workers(self, session, tasks_to_run):

        execution_queue = []

        for ti in tasks_to_run:

            # Check scheduler concurrency limits
            # TODO: Implement this properly

            if len(self.active_tasks) >= TASK_LIMIT:
                self.log.info("Queued %i tasks, will not queue more as there are %i active tasks and the task limit is %i" % (len(execution_queue), len(self.active_tasks), TASK_LIMIT))
                break

            dag = self.dags[ti.dag_id]

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
                "Sending {} {} {} to executor with priority {} and queue {}".format(ti.dag_id, ti.task_id, ti.execution_date, priority, queue)
            )

            ti.state = State.QUEUED
            
            task_key = ti.dag_id, ti.task_id, ti.execution_date
            self.active_tasks.add(task_key)

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

        self.log.info("Executor now has %i tasks queued" % (len(self.active_tasks)))
        

    def run_dependency_checks(self, session):

        # Find the task instances we want to run depdency checks for. 

        # We retieve the task keys from the database and retain them for the next stages. It's possble
        # that this could be avoided, and the depdency information and task instances are retrieved in 
        # a single select using some fairly complex subqueries. 
        
        # Note if you are doing this entirely in the database, you would need to use a single select 
        # statement or set isolation level and deal with phantom reads / non-repeatable reads, as other
        # parts of the codebase make changes to the TaskInstance table

        # For now just doing this by reading and storing the keys. Subqueries etc would also require 
        # more database testing for sqlite and mysql

        task_keys = list(session.query(
                        models.TaskInstance.dag_id, 
                        models.TaskInstance.task_id, 
                        models.TaskInstance.execution_date
                    )
                    .filter(models.TaskInstance.state == State.NONE))

        self.log.info("Analysing dependencies for %i task instances" % len(task_keys))

        tasks_to_queue = []

        def chunks(list_, n):
            for i in range(0, len(list_), n):
                yield list_[i:i + n]

        # We chunk by 500 items at a time to avoid sql errors 

        for task_key_slice in chunks(task_keys, 500):

            # Find the state of all previous task instances
            # This gets a maping in the form
            #   (dag_id, task_id, execution_date): previous_task_instance_state

            # TODO: This could be filtered by task instances where the task has depends_on_past = True
            # which would reduce the amount of data loaded from the database

            previous_task_state_map = self._get_previous_task_states(session, task_key_slice)
     
            # Find the state of all other task instances in the same dag, for each task 
            # This returns (dag_id, execution_date) -> {task_id: state, task_id: state, ...} 
            #   for all tasks in the dag runs in the task instance list

            # TODO: This could be filtered by task instances where there are dependencies on other tasks

            dr_ti_state_map = self._get_related_ti_states(session, task_key_slice)
            
            # Now process each task instance in turn
     
            query_tis = (session.query(models.TaskInstance)
                            .filter(
                                tuple_(
                                    models.TaskInstance.dag_id,
                                    models.TaskInstance.task_id,
                                    models.TaskInstance.execution_date
                                ).in_(task_key_slice)
                            ))

            for ti in query_tis:

                dag = self.dags[ti.dag_id]
                task = dag.get_task(ti.task_id)

                # Get the previous task instance state and related task instance states for this task
                try:
                    prev_task_date, prev_task_state = previous_task_state_map[(ti.dag_id, ti.task_id, ti.execution_date)]
                    dag_task_states = dr_ti_state_map[(ti.dag_id, ti.execution_date)]
                except Exception as e:
                    # We should not get a keyerror here as we are freezing the list of task keys at the start of this function
                    self.log.error("Error querying the state for {} {} {} - please report!".format(ti.dag_id, ti.task_id, ti.execution_date))
                    raise
                    
                parent_task_states = set([dag_task_states[parent] for parent in task.upstream_task_ids])

                self.log.debug("Dependency check for {} {} {}".format(ti.dag_id, ti.task_id, ti.execution_date))
                self.log.debug("Previous task instance: %s -> %s" % (prev_task_date, prev_task_state))
                self.log.debug("States of other tasks in the same DagRun: %s" % str(dag_task_states))

                # Check if depends_on_past is set, and if the previous task has run yet
                # If there is no previous task instance, then queue this task. This situation should only be reached
                # for the very first task instance in a dag

                if task.depends_on_past:
                    if prev_task_date is not None and prev_task_state == 'failed':
                        self.log.debug("Setting task %i to state 'upstream_failed' as depends_on_past set true and previous task failed")
                        ti.state = State.FAILED
                        continue

                    if prev_task_date is not None and prev_task_state != 'success':
                        self.log.debug("Leaving task %i in state 'queued' as depends_on_past set true and previous task hasn't executed")
                        continue

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
                            ti.state = State.UPSTREAM_FAILED
                            self.log.debug("Setting task %s to state 'upstream_failed' as the trigger rule is 'all_success' and there are upstream tasks in a failed state" % ti)
                            continue
                        else:
                            state_str = ','.join(sorted("'%s'" % s for s in non_success_states))
                            self.log.debug("Leaving task %s in state 'queued' as the trigger rule is 'all_success' but there are tasks in states %s" % (ti, state_str))
                            continue

                if task.trigger_rule == models.TriggerRule.ALL_FAILED:
                    non_failure_states = parent_task_states - set([State.FAILURE])
                    if non_failure_states:
                        final_non_failure_states = parent_task_states & (FINAL_STATES - set([State.FAILURE]))
                        if len(final_non_failure_states) > 0:
                            ti.state = State.SKIPPED
                            self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'all_failure' and there are upstream tasks which did not fail" % ti)
                            continue
                        else:
                            state_str = ','.join(sorted("'%s'" % s for s in non_failure_states))
                            self.log.debug("Leaving task %s in state 'queued' as the trigger rule is 'all_failure' but there are tasks in states %s" % (ti, state_str))
                            continue

                if task.trigger_rule == models.TriggerRule.ONE_SUCCESS:
                    if State.SUCCESS not in parent_task_states:
                        if len(parent_task_states - FINAL_STATES) == 0:
                            ti.state = State.SKIPPED
                            self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'one_success' and all upstream tasks are finished, but no tasks have succeeded" % ti)
                            continue
                        else:
                            self.log.debug("Leaving task %s in state 'queued' as the trigger rule is 'one_success' but no tasks have succeeded" % ti)
                            continue

                if task.trigger_rule == models.TriggerRule.ONE_FAILED:
                    if State.FAILURES not in parent_task_states:
                        if len(parent_task_states - FINAL_STATES) == 0:
                            ti.state = State.SKIPPED
                            self.log.debug("Setting task %s to state 'skipped' as the trigger rule is 'one_failure' and all upstream tasks are finished, but no tasks have failed" % ti)
                            continue
                        else:
                            self.log.debug("Leaving task %s in state 'queued' as the trigger rule is 'one_failure' but no tasks have failed" % ti)
                            continue
                    
                # If we reach here then the task is ready to be sent to the executor

                tasks_to_queue.append(ti)

        self.log.info("Dependency checks complete, have %i tasks ready to execute" % len(tasks_to_queue))

        return tasks_to_queue
    


    def build_dagruns_and_tasks(self, session):

        for dag_id in self.dagbag.dags:

            dag = self.dags[dag_id]

            if dag.is_subdag:
                # Don't create DagRun or TaskInstance objects for SubDags. They get created by the SubDagOperator
                # which is a bit odd, but we'll leave it for now during this refactor
                continue

            if dag.is_paused:
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

            # To keep track of how many new objects we create
            dr_count = 0
            ti_count = 0

            while next_run_date: 

                if dr_count == self.MAX_CREATE_DAGRUN_PER_LOOP:
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



    def _get_previous_task_states(self, session, task_keys):
    
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

        # Create query to get the task instances in task_keys

        query_tis = (session.query(models.TaskInstance)
                        .filter(
                            tuple_(
                                models.TaskInstance.dag_id, 
                                models.TaskInstance.task_id, 
                                models.TaskInstance.execution_date  
                            ).in_(task_keys)
                        )).subquery()

        # Join the previous table instance

        prev_task_instance = aliased(models.TaskInstance)

        prev_execution_date = (session.query(func.max(prev_task_instance.execution_date))
                                .filter(prev_task_instance.execution_date < query_tis.c.execution_date)
                                .filter(prev_task_instance.task_id == query_tis.c.task_id)
                                .filter(prev_task_instance.dag_id == query_tis.c.dag_id)
                                .label("prev_execution_date"))

        ti_and_prev_tis = session.query(query_tis.c.dag_id, query_tis.c.task_id, query_tis.c.execution_date, prev_execution_date)

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

        previous_task_state_map = {
            (dag_id, task_id, execution_date): (prev_execution_date, previous_state)
            for (dag_id, task_id, execution_date, prev_execution_date, previous_state) in query
        }
    
        return previous_task_state_map

    def _get_related_ti_states(self, session, task_keys):
        """ Returns a mapping of (dag_id, execution_date) -> task_id -> state
              eg: ('test_dag', '2018-04-22 00:00:00') : { 'task_1': 'success', 'task_2': 'queued', 'task_3': 'failure'}

            For all DagRuns related to the query TaskInstances (query_tis)
        """

        query_tis = (session.query(models.TaskInstance)
                        .filter(
                            tuple_(
                                models.TaskInstance.dag_id,
                                models.TaskInstance.task_id,
                                models.TaskInstance.execution_date
                            ).in_(task_keys)
                        )).subquery()

        ti_states = (session.query(models.TaskInstance.dag_id, models.TaskInstance.task_id, models.TaskInstance.execution_date, models.TaskInstance.state)
            .filter(models.TaskInstance.execution_date == models.DagRun.execution_date)
            .filter(models.TaskInstance.dag_id == models.DagRun.dag_id)
            .filter(query_tis.c.execution_date == models.DagRun.execution_date)
            .filter(query_tis.c.dag_id == models.DagRun.dag_id)
            .distinct())
        
        dr_ti_state_map = defaultdict(dict)
        for (dag_id, task_id, execution_date, state) in ti_states:
            dr_ti_state_map[(dag_id, execution_date)][task_id] = state

        return dict(dr_ti_state_map)  # convert the defaultdict into a dict
         
    def shutdown(self):
        print ("Instructing scheduler to shut down")
        self.shutdown_status = True

