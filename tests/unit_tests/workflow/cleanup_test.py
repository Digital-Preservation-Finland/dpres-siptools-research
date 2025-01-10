"""Tests for :mod:`siptools_research.workflow.cleanup` module."""
import pytest

from siptools_research.workflow.cleanup import Cleanup


@pytest.mark.usefixtures("testmongoclient")
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
