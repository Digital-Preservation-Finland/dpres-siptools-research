"""Tests for :mod:`siptools_research.workflow.cleanup` module"""
import os
import pytest

import tests.conftest
from siptools_research.utils.database import Database
from siptools_research.workflow.cleanup import CleanupWorkspace
from metax_access.metax import DatasetNotFoundError


@pytest.mark.usefixtures("testmongoclient")
def test_cleanupworkspace(testpath, requests_mock):
    """Test that task.run() removes workspace.

    :param testpath: Temporary directory fixture
    :param requests_mock
    """

    requests_mock.get(
        "https://metaksi/rest/v1/datasets/identifier/files",
        json=[])

    workspace, task, database = _do_setup(testpath)

    # Running the task should remove the workspace, but the task should not yet
    # be complete, because no there is no information of workflow in database.
    assert os.path.exists(workspace)
    task.run()
    _assert_workspace_cleaned(workspace, database, task)


@pytest.mark.usefixtures("testmongoclient")
def test_cleanupworkspace_missing_dataset(testpath, requests_mock):
    """Test that task.run() removes workspace although dataset not found in
    metax.

    :param testpath: Temporary directory fixture
    :param requests_mock
    """
    # mock metax to raise MetaxError meaning dataset was not found
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/identifier/files",
        exc=DatasetNotFoundError)

    workspace, task, database = _do_setup(testpath)

    # Running the task should remove the workspace, but the task should not yet
    # be complete, because no there is no information of workflow in database.
    assert os.path.exists(workspace)
    task.run()
    _assert_workspace_cleaned(workspace, database, task)


def _do_setup(testpath):
    """Sets up testing environment.

    :param testpath: Temporary directory fixture
    :return workspace, task, database
    """
    # Create a workspace directory
    workspace = os.path.join(testpath, 'test_workspace')
    os.makedirs(workspace)
# Init database client
    database = Database(tests.conftest.UNIT_TEST_CONFIG_FILE)
# Init task
    task = CleanupWorkspace(workspace=workspace, dataset_id='identifier',
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    return workspace, task, database


def _assert_workspace_cleaned(workspace, database, task):
    """Asserts that workspace was cleaned and the task remains as incomplete
    until the required task ReportPreservationStatus succeeds

    :param workspace: Temporary directory fixture
    :param database: Database object providing access to workflow task database
    :param task CleanupWorkspace object under test
    """
    assert not os.path.exists(workspace)
    assert not task.complete()
    # The task should be incomplete when ReportPreservationStatus task has not
    # yet run
    database.add_workflow(os.path.basename(workspace), 'test_id')
    assert not task.complete()
    # The task should be incomplete when ReportPreservationStatus task has
    # failed
    database.add_event(os.path.basename(workspace), 'ReportPreservationStatus',
                       'failure',
                       'Preservation state could not be reported.')
    assert not task.complete()
    # Task should be complete when ReportPreservationStatus task has run
    # succesfully
    database.add_event(os.path.basename(workspace), 'ReportPreservationStatus',
                       'success',
                       'Lets pretend that all other wf-tasks are completed')
    assert task.complete()
