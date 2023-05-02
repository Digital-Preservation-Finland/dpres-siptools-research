"""Tests for :mod:`siptools_research.workflow_init` module."""
import pytest

import tests.conftest
import siptools_research
from siptools_research.exceptions import WorkflowExistsError
from siptools_research.workflow_init import (generate_metadata,
                                             validate_metadata,
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
def test_validate_metadata():
    """Test validate_metadata function.

    Tests that `validate_metadata` schedules correct task.

    :returns: ``None``
    """
    validate_metadata('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that workflow was added to database.
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert len(database.get_all_active_workflows()) == 1
    workflow = database.get_all_active_workflows()[0]
    assert workflow['dataset'] == 'dataset1'
    assert workflow['target_task'] == 'ReportMetadataValidationResult'


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
def test_workflow_conflict():
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
    preserve_dataset('dataset1', config=tests.conftest.UNIT_TEST_CONFIG_FILE)
