"""Tests for :mod:`siptools_research.workflow_init` module."""
from unittest import mock

import pytest

import tests.conftest
import siptools_research

from siptools_research.workflow_init import preserve_dataset


def test_initworkflow():
    """Test that ``InitWorkflow.requires`` function returns task with correct
    parameters.

    :returns: ``None``
    """
    task = siptools_research.workflow_init.InitWorkflow(
        workspace='test_workspace',
        dataset_id='test_dataset',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    required_task = task.requires()

    assert required_task.workspace == 'test_workspace'
    assert required_task.dataset_id == 'test_dataset'
    assert required_task.__class__.__name__ == 'CleanupWorkspace'


@pytest.mark.usefixtures('testmongoclient')
def test_initworkflows():
    """Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces CleanupWorkflow tasks for each
    incomplete workflow in database.

    :returns: ``None``
    """
    # Add sample workflows to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'dataset1')
    database.add_workflow('workflow2', 'dataset2')
    database.set_completed('workflow2')
    database.add_workflow('workflow3', 'dataset3')

    # Get list of tasks required by InitWorkflows task
    task = siptools_research.workflow_init.InitWorkflows(
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    required_tasks = list(task.requires())

    # Only workflows 1 and 3 should be incomplete
    assert {required_task.workspace for required_task in required_tasks} == {
        './test_packaging_root/workspaces/workflow1',
        './test_packaging_root/workspaces/workflow3'
    }


@pytest.mark.usefixtures('testmongoclient')
@mock.patch('subprocess.Popen')
def test_preserve_dataset_sets_preservation_state(mock_subproc_popen,
                                                  requests_mock):
    """Tests that dataset's preservation_state and preservation_description
    attributes are set correctly.

    :param mock_subproc_popen: Mocked subprocess.Popen
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch("https://metaksi/rest/v2/datasets/dataset_1")

    preserve_dataset('dataset_1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert mock_subproc_popen.called
    json_message = requests_mock.last_request.json()
    assert json_message['preservation_state'] == 90
    assert json_message['preservation_description'] == 'In packaging service'
