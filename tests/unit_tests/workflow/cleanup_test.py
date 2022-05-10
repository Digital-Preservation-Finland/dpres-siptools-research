"""Tests for :mod:`siptools_research.workflow.cleanup` module."""
import pytest

import tests.conftest
from siptools_research.utils.database import Database
from siptools_research.workflow.cleanup import CleanupWorkspace


@pytest.mark.parametrize("metax_status_code", (200, 404))
@pytest.mark.usefixtures("testmongoclient")
def test_cleanupworkspace(workspace, requests_mock, metax_status_code):
    """Test that task.run() removes workspace.

    Cleanup should work also when dataset is not available in Metax.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    requests_mock.get("https://metaksi/rest/v2/datasets/identifier/files",
                      json=[],
                      status_code=metax_status_code)

    # Init database client
    database = Database(tests.conftest.UNIT_TEST_CONFIG_FILE)
    # Init task
    task = CleanupWorkspace(workspace=str(workspace), dataset_id='identifier',
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Running the task should remove the workspace, but the task should
    # not yet be complete, because no there is no information of
    # workflow in database.
    assert workspace.is_dir()
    task.run()
    assert not workspace.is_dir()
    assert not task.complete()

    # The task should be incomplete when ReportPreservationStatus task
    # has not yet run
    database.add_workflow(workspace.name, 'test_id')
    assert not task.complete()

    # The task should be incomplete when ReportPreservationStatus task
    # has failed
    database.add_task(
        workspace.name, 'ReportPreservationStatus',
        'failure',
        'Preservation state could not be reported.'
    )
    assert not task.complete()

    # Task should be complete when ReportPreservationStatus task has run
    # succesfully
    database.add_task(workspace.name, 'ReportPreservationStatus',
                      'success',
                      'Lets pretend that all other wf-tasks are completed')
    assert task.complete()
