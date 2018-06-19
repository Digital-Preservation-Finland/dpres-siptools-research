"""Tests for ``siptools_research.workflow.cleanup`` module"""
import os
import pytest
import tests.conftest
import tests.conftest
from siptools_research.utils.database import Database
from siptools_research.workflow.cleanup import CleanupWorkspace

@pytest.mark.usefixtures("testmongoclient")
def test_cleanupworkspace(testpath):
    """Test that task.run() removes workspace.

    :testpath: Temporary directory fixture"""

    # Create a workspace directory and some content into it
    workspace = os.path.join(testpath, 'test_workspace')
    os.makedirs(os.path.join(workspace, 'logs'))
    with open(os.path.join(workspace, 'logs', 'test.log'), 'w+') as output:
        output.write('foo')

    # Init task
    task = CleanupWorkspace(workspace=workspace,
                            dataset_id='1',
                            config=tests.conftest.TEST_CONFIG_FILE)

    # The workspace should exists before task has been run
    assert not task.complete()
    assert os.path.exists(workspace)

    # Task should not yet be complete, because ReportPreservationStatus task
    # has not been run succesfully
    task.run()
    assert not task.complete()

    # After running task the workspace directory should have disappeared
    assert not os.path.exists(workspace)

    # Manipulate workflow database
    Database(tests.conftest.TEST_CONFIG_FILE).add_event(
        os.path.basename(workspace),
        'ReportPreservationStatus',
        'success',
        'Lets pretend that all other wf-tasks are completed'
    )

    # Now task should be complete
    assert not task.complete()
