"""Tests for :mod:`siptools_research.tasks.cleanup` module."""
from siptools_research.tasks.cleanup import Cleanup


def test_cleanup(config, workspace):
    """Test that task.run() removes the workspace.

    :param config: Configuration file
    :param workspace: Temporary workspace directory fixture
    """
    # Init task
    task = Cleanup(dataset_id=workspace.name, config=config)

    assert not task.complete()
    task.run()
    assert task.complete()

    # The workspace should be removed
    assert not workspace.exists()
