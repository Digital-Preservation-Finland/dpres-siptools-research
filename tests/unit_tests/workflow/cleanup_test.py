"""Tests for :mod:`siptools_research.workflow.cleanup` module."""
import pytest

import tests.conftest
from siptools_research.workflow.cleanup import Cleanup


@pytest.mark.usefixtures("testmongoclient")
def test_cleanup(workspace):
    """Test that task.run() removes the workspace.

    :param workspace: Temporary workspace directory fixture
    """
    # Init task
    task = Cleanup(dataset_id=workspace.name,
                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert not task.complete()
    task.run()
    assert task.complete()

    # The workspace should be removed
    assert not workspace.exists()
