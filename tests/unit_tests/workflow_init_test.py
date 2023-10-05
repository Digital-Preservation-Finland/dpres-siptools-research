"""Tests for :mod:`siptools_research.workflow_init` module."""
import pytest
from metax_access import DS_STATE_METADATA_CONFIRMED

import tests.conftest
import siptools_research
from siptools_research.exceptions import WorkflowExistsError
from siptools_research.workflow_init import (generate_metadata,
                                             validate_dataset,
                                             preserve_dataset,
                                             InitWorkflows)


@pytest.mark.usefixtures('testmongoclient')
def test_initworkflows():
    """Test InitWorkflows task.

    Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces Tasks
    for each incomplete workflow in database.

    :returns: ``None``
    """
    # Add sample workflows to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'CleanupFileCache', 'dataset1')
    database.add_workflow('workflow2', 'CleanupFileCache', 'dataset2')
    database.set_completed('workflow2')
    database.add_workflow('workflow3', 'CleanupFileCache', 'dataset3')

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
def test_generate_metadata():
    """Test generate_metadata function.

    Tests that `generate_metadata` schedules correct task.

    :returns: ``None``
    """
    generate_metadata('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_all_active_workflows()) == 1
    workflow = database.get_all_active_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'GenerateMetadata'


@pytest.mark.usefixtures('testmongoclient')
def test_validate_dataset():
    """Test validate_dataset function.

    Tests that `validate_dataset` schedules correct task.

    :returns: ``None``
    """
    validate_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_all_active_workflows()) == 1
    workflow = database.get_all_active_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'ReportDatasetValidationResult'


@pytest.mark.usefixtures('testmongoclient')
def test_preserve_dataset():
    """Test preserve_dataset function.

    Tests that `preserve_dataset` schedules correct task.

    :returns: ``None``
    """
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_all_active_workflows()) == 1
    workflow = database.get_all_active_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'CleanupFileCache'


@pytest.mark.usefixtures('testmongoclient')
def test_workflow_conflict(requests_mock):
    """Test starting another workflow for dataset.

    Tests that that new workflows can not be started when dataset
    already has an active workflow.

    :returns: ``None``
    """
    # Add a sample workflow to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'ReportMetadataValidationResult',
                          'dataset1')

    # Try to start another workflow
    with pytest.raises(WorkflowExistsError):
        preserve_dataset('dataset1',
                         config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # New workflows should not be created and the existing workflow
    # should not be changed
    workflows = database.get_all_active_workflows()
    assert len(workflows) == 1
    assert not workflows[0]['disabled']
    assert workflows[0]['target_task'] == 'ReportMetadataValidationResult'
    assert workflows[0]['dataset'] == 'dataset1'

    # New workflow can be started when the previous workflow is
    # completed
    database.set_completed('workflow1')
    requests_mock.get('/rest/v2/datasets/dataset1',
                      json={'preservation_state': DS_STATE_METADATA_CONFIRMED})
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Only the target_task of the workflow should be updated
    workflows = database.get_all_active_workflows()
    assert len(workflows) == 1
    assert not workflows[0]['disabled']
    assert workflows[0]['target_task'] == 'CleanupFileCache'
    assert workflows[0]['dataset'] == 'dataset1'


@pytest.mark.parametrize('previous_workflow_is_completed', [True, False])
@pytest.mark.usefixtures('testmongoclient')
def test_continuing_disabled_workflow(previous_workflow_is_completed):
    """Test that previous disabled workflow is not continued.

    New workflow should always be created, if previous worklow of the
    dataset is disabled. It does not matter if the previous workflow is
    complete or not.

    :param previous_workflow_is_completed: `True`, if previous workflow
                                           is enabled
    :returns: ``None``
    """
    # Add a disabled incomplete workflow to database
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    database.add_workflow('workflow1', 'PreviousTarget',
                          'dataset1')
    database.set_disabled('workflow1')
    if previous_workflow_is_completed:
        database.set_completed('workflow1')

    # Preserve the dataset that has previous disabled task
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # The previous workflow should not be changed
    previous_workflow = database.get_one_workflow('workflow1')
    assert previous_workflow['target_task'] == 'PreviousTarget'
    assert previous_workflow['disabled'] is True
    assert previous_workflow['completed'] is previous_workflow_is_completed
    assert len(database.get_workflows('dataset1')) == 2
