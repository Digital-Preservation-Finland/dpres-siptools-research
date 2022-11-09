"""Tests for :mod:`siptools_research.workflow_init` module."""
import pytest

import tests.conftest
import siptools_research

from siptools_research.workflow_init import (preserve_dataset,
                                             InitWorkflow,
                                             InitWorkflows)


def test_initworkflow():
    """Test InitWorkflow task.

    ``InitWorkflow.requires`` function returns task with correct
    parameters.

    :returns: ``None``
    """
    task = InitWorkflow(
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
    """Test InitWorkflows task.

    Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces CleanupWorkflow tasks
    for each incomplete workflow in database.

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
    task = InitWorkflows(
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    required_tasks = list(task.requires())

    # Only workflows 1 and 3 should be incomplete
    assert {required_task.workspace for required_task in required_tasks} == {
        './test_packaging_root/workspaces/workflow1',
        './test_packaging_root/workspaces/workflow3'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_preserve_dataset(mocker, requests_mock):
    """Test preserve_dataset function.

    Tests that `preserve_dataset` schedules correct task, and sets the
    preservation_state and preservation_description attributes of the
    dataset.

    :param mocker: Pytest-mock mocker
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock luigi
    mock_luigi = mocker.patch('luigi.build')

    # Mock Metax
    requests_mock.patch("/rest/v2/datasets/dataset_1")

    # Preserve dataset
    preserve_dataset('dataset_1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that one task was scheduled with correct parameters
    mock_luigi.assert_called_once()
    # TODO: use mock_luigi.call_args.args in Python 3.8
    scheduled_tasks = mock_luigi.call_args[0][0]
    assert len(scheduled_tasks) == 1
    assert isinstance(scheduled_tasks[0], InitWorkflow)
    assert scheduled_tasks[0].workspace.startswith(
        "./test_packaging_root/workspaces/aineisto_dataset_1-"
    )
    assert scheduled_tasks[0].dataset_id == "dataset_1"
    assert scheduled_tasks[0].config == ("tests/data/configuration_files"
                                         "/siptools_research_unit_test.conf")

    # Check that correct luigi configuration file was used
    # TODO: use mock_luigi.call_args.kwargs in Python 3.8
    assert mock_luigi.call_args[1] \
        == {'logging_conf_file': '/etc/luigi/research_logging.cfg'}

    # Check that preservation state was changed
    json_message = requests_mock.last_request.json()
    assert json_message['preservation_state'] == 90
    assert json_message['preservation_description'] == 'In packaging service'
