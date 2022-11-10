"""Tests for :mod:`siptools_research.workflow_init` module."""
import pytest

from metax_access import (DS_STATE_INITIALIZED,
                          DS_STATE_VALIDATED_METADATA_UPDATED,
                          DS_STATE_VALIDATING_METADATA,
                          DS_STATE_VALID_METADATA)

import tests.conftest
import siptools_research
from siptools_research.workflow_init import (package_dataset,
                                             preserve_dataset,
                                             InitWorkflows)


@pytest.mark.usefixtures('testmongoclient')
def test_initworkflows():
    """Test InitWorkflows task.

    Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces CleanupWorkspace tasks
    for each incomplete workflow in database.

    :returns: ``None``
    """
    # Add sample workflows to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'CleanupWorkspace', 'dataset1')
    database.add_workflow('workflow2', 'CleanupWorkspace', 'dataset2')
    database.set_completed('workflow2')
    database.add_workflow('workflow3', 'CleanupWorkspace', 'dataset3')

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
def test_package_dataset(requests_mock):
    """Test package_dataset function.

    Tests that `package_dataset` schedules correct task.

    :returns: ``None``
    """
    # Mock metax
    requests_mock.get('/rest/v2/datasets/dataset1',
                      json={'identifier': 'dataset1'})

    # Package dataset
    package_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_incomplete_workflows()) == 1
    workflow = database.get_incomplete_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'ReportPackagingStatus'


@pytest.mark.usefixtures('testmongoclient')
def test_preserve_dataset(requests_mock):
    """Test preserve_dataset function.

    Tests that `preserve_dataset` schedules correct task.

    :returns: ``None``
    """
    # Mock metax
    requests_mock.get('/rest/v2/datasets/dataset1',
                      json={'identifier': 'dataset1'})

    # Preserve dataset
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_incomplete_workflows()) == 1
    workflow = database.get_incomplete_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'CleanupWorkspace'


@pytest.mark.parametrize(
    (
        'preservation_state',
        'previous_workflow_disabled',
        'previous_target_task'),
    [
        (DS_STATE_VALIDATED_METADATA_UPDATED, True, 'ReportPackagingStatus'),
        (DS_STATE_VALID_METADATA, False, 'CleanupWorkspace'),
        (DS_STATE_VALIDATING_METADATA, False, 'CleanupWorkspace')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_continue_existing_workflow(requests_mock, preservation_state,
                                    previous_workflow_disabled,
                                    previous_target_task):
    """Test continuing existing workflow.

    Tests that `preserve_dataset` continues previous packaging workflow
    if dataset metadata has not been changed.

    :param preservation_state: Preservation state of dataset
    :param previous_workflow_disabled: Should the previous workflow be
                                       disabled aftger preservation
                                       workflow is initialized?
    :param previous_target_task: The target task of previous workflow
                                 after after preservation workflow has
                                 been initialized.
    :returns: ``None``
    """
    # Add a sample workflow to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'ReportPackagingStatus', 'dataset1')

    # Mock Metax.
    requests_mock.get('/rest/v2/datasets/dataset1',
                      json={'identifier': 'dataset1',
                            'preservation_state': preservation_state})

    # Preserve dataset
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # The previous should have expected attributes
    previous_workflow = database.get_one_workflow('workflow1')
    assert previous_workflow['disabled'] == previous_workflow_disabled
    assert previous_workflow['target_task'] == previous_target_task

    # The previous workflow should be disabled if it can not be
    # continued, so there should always be only one enabled workflow.
    assert len(database.get_incomplete_workflows()) == 1
    workflow = database.get_incomplete_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'CleanupWorkspace'


@pytest.mark.usefixtures('testmongoclient')
@pytest.mark.parametrize('dataset_to_preserve', ['dataset1', 'dataset2'])
def test_preserve_pas_catalog_version(requests_mock, dataset_to_preserve):
    """Test that workflow is started for PAS data catalog version.

    If there is many versions of the dataset, the version in PAS data
    catalog should be packaged and preserved.

    :param rquests_mock: HTTP request mocker
    :param dataset_to_preserve: the identifier of dataset to be
                                preserved
    :returns: ``None``
    """
    # Mock Metax.
    requests_mock.get(
        '/rest/v2/datasets/dataset1',
        json={
            'identifier': 'dataset1',
            'preservation_state': DS_STATE_INITIALIZED,
            'preservation_dataset_version': {'identifier': 'dataset2'}
        }
    )
    requests_mock.get(
        '/rest/v2/datasets/dataset2',
        json={
            'identifier': 'dataset2',
            'preservation_state': DS_STATE_VALID_METADATA,
            'preservation_dataset_origin_version': {'identifier': 'dataset1'}
            }
    )

    # Add a sample workflow to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'ReportPackagingStatus', 'dataset1')

    # Preserve dataset
    preserve_dataset(dataset_to_preserve,
                     config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # The existing workflow should be updated
    workflow = database.get_one_workflow('workflow1')
    assert not workflow['disabled']
    assert workflow['dataset'] == 'dataset2'
    assert workflow['target_task'] == 'CleanupWorkspace'
