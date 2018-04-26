"""
DummyExecutor just pretends to run tasks
Sadly it cannot handle SubDagOperator or DagRunOperator
"""

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.settings import Session
from airflow import models

from sqlalchemy import and_

import multiprocessing


class DummyExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """

    def start(self):
        pass

    def execute_async(self, key, command, queue=None, executor_config=None):
        
        dag_id, task_id, execution_date = key

        print("Pretending to run {} {} {}".format(dag_id, task_id, execution_date))

        # Just update the database to say we are complete

        session = Session()
        (session.query(models.TaskInstance)
            .filter(
                and_(
                    models.TaskInstance.dag_id == dag_id,
                    models.TaskInstance.task_id == task_id,
                    models.TaskInstance.execution_date == execution_date
                )
            )
        ).update({'state': State.SUCCESS})

        self.result_queue.put((key, State.SUCCESS))
        
    def start(self):
        self.result_queue = multiprocessing.Queue()

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        pass
