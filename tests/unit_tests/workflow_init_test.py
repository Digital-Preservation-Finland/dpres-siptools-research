"""Tests for :mod:`siptools_research.workflow_init` module"""
import json

import pytest
import mock
import httpretty

import siptools_research
from siptools_research.workflow_init import preserve_dataset
import tests.conftest


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
    assert set([required_task.workspace for required_task in required_tasks])\
        == set(['./test_packaging_root/workflow1',
                './test_packaging_root/workflow3'])


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
@mock.patch('subprocess.Popen')
# pylint: disable=invalid-name
def test_preserve_dataset_sets_preservation_state(mock_subproc_popen):
    """Tests that dataset's preservation_state and preservation_description
    attributes are set correctly..

    :returns: ``None``
    """
    preserve_dataset('dataset_1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert mock_subproc_popen.called
    json_message = json.loads(httpretty.last_request().body)
    assert json_message['preservation_state'] == 90
    assert json_message['preservation_description'] == 'In packaging service'


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
@mock.patch('subprocess.Popen')
# pylint: disable=invalid-name
def test_preserve_dataset_only_description(mock_subproc_popen):
    """Verifies that only preservation_description attribute is set if not
    already correct.

    :returns: ``None``
    """
    preserve_dataset(
        'dataset_1_in_packaging_service_with_conflicting_description',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert mock_subproc_popen.called
    json_message = json.loads(httpretty.last_request().body)
    assert 'preservation_state' not in json_message
    assert json_message['preservation_description'] == 'In packaging service'
    httpretty.reset()


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
@mock.patch('subprocess.Popen')
# pylint: disable=invalid-name
def test_preserve_dataset_no_changes(mock_subproc_popen):
    """Verifies that metax is not called to set preservation_state or
    preservation_description attributes when they already have correct values

    :returns: ``None``
    """
    preserve_dataset('dataset_1_in_packaging_service',
                     config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert mock_subproc_popen.called
    # Check that metax set_preservation_state is not called
    assert isinstance(httpretty.last_request(),
                      httpretty.core.HTTPrettyRequestEmpty)
