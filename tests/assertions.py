"""Common assertions for all tests"""

import subprocess
import xml.etree.ElementTree as ET
import datetime

import logging
import os
import re
import time
import fnmatch

import pytest

import scandir

from luigi.worker import Worker

from tests.conftest import scheduler

from siptools_research.workflow.utils import iter_transfers


LOGGER = logging.getLogger('tests.assertions')

REGEX_UUID4 = '[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-' \
    '?[89ab][a-f0-9]{3}-?[a-f0-9]{12}'


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


def check_transfer_complete(workspace):
    """Check ProcessTransfers task has completed succesfully

    :workspace: Path to sip workspace

    """

    assert len_iterable(iter_transfers(workspace)) == 1

    assert os.path.isfile(os.path.join(
        workspace, 'reports',
        'task-report-move-transfer-to-workspace.xml'))



def check_workspace_removed(request, unique, timeout=10):
    """Check that workspace with given `unique` was removed.

    :request: Pytest request fixture
    :unique: Unique identifier for workspace
    :returns: None

    """
    check_directory_does_not_exist(
        '*%s*' % unique, request.config.getoption('--workspace-path'), timeout)
