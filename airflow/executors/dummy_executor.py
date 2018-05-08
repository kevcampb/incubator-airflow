"""
DummyExecutor just pretends to run tasks
Sadly it cannot handle SubDagOperator or DagRunOperator
"""

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.helpers import chunks
from airflow import models, configuration

from sqlalchemy import and_, tuple_

import datetime
import random
import time


class DummyExecutor(BaseExecutor):
    """ This is a fake exceutor for testing purposes. It will not run any operator code and instead return
        success for all tasks

        Note that this will currently not work well with the following 

          ShortCircuitOperator  
          BranchPythonOperator
          TriggerDagRunOperator
          SubDagOperator

        All of those intrinsicly need to run code to modify database state in the current implementation

        The tasks flow through 3 queues during processing
    
            1. queued_tasks  (from BaseOperator, which manages this)
                 respecting parallelism limit, calls execute_async

            2. task_queue    (task_key, planned_completion_timestamp)
                 when we call sync we set database state on tasks to running / success as approprate

            3. running_queue (task_key, planned_completion_date)
                 we check this and see if the task has finished by timestamp and set state to success
    """

    def start(self):
        pass

    def start(self):

        # Incoming tasks to process, stored as (key, completion_timestamp) 
        # We calculate a stochastic completion timestamp in advance when the task is raised
        self.task_queue = []

        # We move tasks from the task_queue to the running_queue after we set the database
        # state to running
        self.running_queue = []

        # Set whenever we run a heartbeat
        self.latest_heartbeat = None

    def execute_async(self, key, command, queue=None, executor_config=None):
        """ key is (dag_id, task_id, execution_date) """

        mu = 60         # Mean is 60 seconds
        sigma = 12      # Some variance so we get roughtly between 30-90 seconds

        duration = random.normalvariate(mu, sigma)
        duration = min(duration, 240)  # Maximum task duration 4 minutes
        duration = max(duration, 10)   # Minimum task duration 10 seconds

        completion_time = timezone.utcnow() + datetime.timedelta(seconds=duration)

        self.task_queue.append((key, completion_time))


    def sync(self):

        from airflow.jobs import LocalTaskJob

        st = time.time()

        # Freezing this so all objects get the same value on this update
        # in order to make the database more readable

        current_time = timezone.utcnow()

        newly_running = []
        newly_complete = []
        new_running_queue = []

        session = Session()
            
        # Check if incoming tasks have already completed by the first sync call

        for task_key, completion_time in self.task_queue:
            if completion_time <= current_time:
                newly_running.append(task_key)
                newly_complete.append(task_key)
            else:
                newly_running.append(task_key)
                new_running_queue.append((task_key, completion_time))
            
        # And see if any of our running tasks have completed

        for task_key, completion_time in self.running_queue:
            if completion_time <= current_time:
                newly_complete.append(task_key)
            else:
                new_running_queue.append((task_key, completion_time))
        
        # Load TaskInstance objects for update, where we need to change the state in the database
        # We chunk by 200 items at a time to avoid sql errors and maintain efficiency

        all_keys = newly_complete + newly_running + [key for (key, _) in self.running_queue]

        ti_by_key = {}
        ti_ltj_map = {}
        
        for task_key_slice in chunks(all_keys, 200):

            ti_ltjs = (session.query(models.TaskInstance, LocalTaskJob)
                .outerjoin(LocalTaskJob, models.TaskInstance.job_id == LocalTaskJob.id)
                .filter(
                    tuple_(
                        models.TaskInstance.dag_id,
                        models.TaskInstance.task_id,
                        models.TaskInstance.execution_date
                    ).in_(task_key_slice)
                )
            )

            for ti, ltj in ti_ltjs:

                ti_by_key[(ti.dag_id, ti.task_id, ti.execution_date)] = ti
                if ltj:
                    ti_ltj_map[(ti.dag_id, ti.task_id, ti.execution_date)] = ltj

        # We perioducally update all LocalTaskJobs with a heartbeat

        heartbeat_rate = configuration.getfloat('scheduler', 'JOB_HEARTBEAT_SEC')
        if self.latest_heartbeat is None or self.latest_heartbeat + datetime.timedelta(seconds=heartbeat_rate) < current_time:
            for ltj in ti_ltj_map.values():
                if ltj:
                    ltj.latest_heartbeat = current_time 
            self.latest_heartbeat = current_time

        # Update task states appropriately for runnning / success

        new_ltjs = []

        for dag_id, task_id, execution_date in newly_running:

            ti = ti_by_key[(dag_id, task_id, execution_date)]
            ti.state = State.RUNNING
            self.log.info("Setting task {} {} {} to RUNNING".format(dag_id, task_id, execution_date))

            # Create a LocalTaskJob for the task instance
            # this is required at present for heartbeating

            ltj = LocalTaskJob(
                task_instance = ti,
                state = State.RUNNING,
                latest_heartbeat = current_time,
            )
            session.add(ltj)
            ti_ltj_map[(dag_id, task_id, execution_date)] = ltj
            new_ltjs.append(ltj)
            
        # Flush the newly created LocalTaskJob objects to the database
        # We need to do this, so we can get the autoincrement job_id in order to set this
        # on the task instance

        session.flush(new_ltjs)
    
        for (dag_id, task_id, execution_date), ltj in ti_ltj_map.items():
            ti = ti_by_key[(dag_id, task_id, execution_date)]   
            ti.job_id = ltj.id

        # Now for task which have completed, set their state and the state of their LocalTaskJob

        for dag_id, task_id, execution_date in newly_complete:
            ti = ti_by_key[(dag_id, task_id, execution_date)]
            ti.state = State.SUCCESS
            self.log.info("Setting task {} {} {} to SUCCESS".format(dag_id, task_id, execution_date))
            
            ltj = ti_ltj_map[(dag_id, task_id, execution_date)] 
            ltj.state = State.SUCCESS
            ltj.end_date = current_time

        # Update our running queue for next run
    
        self.running_queue = new_running_queue
        self.task_queue = []

        # Commit everything to the database

        session.commit()

        # Where we decided tasks were complete, we also set the response queue value
        # in the response queue so the scheduler sees the result

        for task_key in newly_complete:
            self.change_state(task_key, State.SUCCESS)

        et = time.time()
        self.log.info("Dummy Executor took %.02f seconds" % (et - st))

    def end(self):
        pass
