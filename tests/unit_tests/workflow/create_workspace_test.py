"""Tests for :mod:`siptools_research.workflow.create_workspace`."""

import pytest
import tests.conftest
from siptools_research.workflow import create_workspace


@pytest.mark.usefixtures('testmongoclient')
def test_createworkspace(workspace):
    """Tests for `CreateWorkspace` task.

    - `Task.complete()` is true after `Task.run()`
    - Directory structure is created in workspace
    - Log entry is created to mongodb

    :param workspace: Test workspace directory
    :returns: ``None``
    """
    workspace.rmdir()
    assert not workspace.is_dir()

    # Init task
    task = create_workspace.CreateWorkspace(
        workspace=str(workspace),
        dataset_id="1",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that directories were created
    assert workspace.is_dir()
    assert (workspace / 'sip-in-progress').is_dir()
