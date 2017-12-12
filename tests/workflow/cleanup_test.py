"""Tests for ``siptools_research.workflow.cleanup`` module"""
import os
from siptools_research.workflow.cleanup import CleanupWorkspace

def test_cleanupworkspace(testpath):
    """Test that task.run() removes workspace."""

    # Create a workspace directory and some content into it
    workspace = os.path.join(testpath, 'test_workspace')
    os.makedirs(os.path.join(workspace, 'logs'))
    with open(os.path.join(workspace, 'logs', 'test.log'), 'w+') as output:
        output.write('foo')

    # Init task
    task = CleanupWorkspace(workspace=workspace,
                            dataset_id='1',
                            config='tests/data/siptools_research.conf')

    # The workspace should exists before task hasbeen run
    assert not task.complete()
    assert os.path.exists(workspace)

    task.run()
    assert task.complete()

    # After running task the workspace directory should have disappeared
    assert not os.path.exists(workspace)
