"""Tests for ``siptools_research.workflow_init`` module"""

import pytest
import siptools_research
import tests.conftest


def test_initworkflow():
    """Test that ``InitWorkflow.requires`` function returns task with correct
    parameters.
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
        == set(['./test_workspace_root/workflow1',
                './test_workspace_root/workflow3'])
