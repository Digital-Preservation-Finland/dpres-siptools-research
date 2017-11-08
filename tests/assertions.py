"""Common assertions for all tests"""
import logging
from luigi.worker import Worker
from tests.conftest import scheduler


LOGGER = logging.getLogger('tests.assertions')


def print_dep_tree(task_id, graph=None, level=0):
    """Print full dependency tree for given task_id"""
    if not graph:
        graph = scheduler().graph()
    task = graph[task_id]
    print "%s%s %s" % (' ' * level, task_id, task["status"])
    if task["status"] == 'FAILED':
        error = scheduler().fetch_error(task_id)["error"]
        for line in error.splitlines():
            print " " * (level + 8), '!', line
        for dep_task_id in task["deps"]:
            print_dep_tree(dep_task_id, graph, level=level+4)


def task_ok(current_task):
    """Run the task and return True, if it was successful.

    :current_task: Task to run and test
    :returns: True if task was successful, else False.

    """

    worker = Worker(scheduler=scheduler())
    worker.add(current_task)
    worker_finished_succesfully = worker.run()
    print_dep_tree(current_task.task_id)
    return worker_finished_succesfully
