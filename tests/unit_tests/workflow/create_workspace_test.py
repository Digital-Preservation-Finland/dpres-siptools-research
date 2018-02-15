"""Test the `siptools_research.workflow.create_workspace` module"""

import os
import pytest
from siptools_research.workflow import create_workspace

@pytest.mark.usefixtures('testmongoclient')
def test_createworkspace(testpath):
    """Tests for `CreateWorkspace` task.

    - `Task.complete()` is true after `Task.run()`
    - Directory structure is created in workspace
    - Log entry is created to mongodb

    :testpath: Testpath fixture
    :returns: None
    """

    workspace = os.path.join(testpath, 'test_workspace')
    assert not os.path.isdir(workspace)

    # Init task
    task = create_workspace.CreateWorkspace(
        workspace=workspace,
        dataset_id="1",
        config='tests/data/siptools_research.conf'
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that directories were created
    assert os.path.isdir(workspace)
    assert os.path.isdir(os.path.join(workspace, 'logs'))
    assert os.path.isdir(os.path.join(workspace, 'sip-in-progress'))
