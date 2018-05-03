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
from airflow import models

from sqlalchemy import and_, tuple_

import datetime
import random


class DummyExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
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

        import time
        st = time.time()

        current_time = timezone.utcnow()

        newly_running = []
        newly_complete = []
        new_running_queue = []

        session = Session()
            
        # Check if incoming tasks have already completed by the first sync call

        for task_key, completion_time in self.task_queue:
            if completion_time <= current_time:
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

        all_keys = newly_complete + newly_running
        ti_by_key = {}
        
        for task_key_slice in chunks(all_keys, 200):

            tis = (session.query(models.TaskInstance)
                .filter(
                    tuple_(
                        models.TaskInstance.dag_id,
                        models.TaskInstance.task_id,
                        models.TaskInstance.execution_date
                    ).in_(task_key_slice)
                )
            )

            for ti in tis:
                ti_by_key[(ti.dag_id, ti.task_id, ti.execution_date)] = ti

        # Update task states appropriately

        for dag_id, task_id, execution_date in newly_running:
            ti = ti_by_key[(dag_id, task_id, execution_date)]
            ti.state = State.RUNNING

        for dag_id, task_id, execution_date in newly_complete:
            ti = ti_by_key[(dag_id, task_id, execution_date)]
            ti.state = State.SUCCESS

        # Update our running queue for next run
    
        self.running_queue = new_running_queue
        self.task_queue = []

        # Where we decided tasks were complete, we also set the response queue value
        # in the response queue so the scheduler sees the result

        for task_key in newly_complete:
            self.change_state(task_key, State.SUCCESS)

        et = time.time()
        print("Dummy Executor took %.02f seconds" % (et - st))

    def end(self):
        pass
