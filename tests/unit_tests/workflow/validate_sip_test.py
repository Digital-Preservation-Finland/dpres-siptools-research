"""Unit tests for :mod:`siptools_research.workflow.validate_sip`."""
from datetime import datetime
import os

import tests.conftest
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.utils.database import Database


def test_validatesip_accepted(testpath, monkeypatch):
    """Initializes validate_sip task and tests that it is not complete.

    Luigi code is then monkeypatched to always think that remote target
    exists.  The task should then be completed.

    :param testpath: Temporary directory fixture
    :param monkeypatch: Monkeypatch fixture
    :returns: ``None``
    """
    monkeypatch.setattr(
        Database, 'get_event_timestamp',
        lambda self, workflow, task: datetime.utcnow().isoformat()
    )
    workspace = os.path.join(testpath, 'workspaces', 'workspace')

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Monkeypatch RemoteFileSystem.exists to return False. The task
    # should not be completed.
    monkeypatch.setattr(
        'siptools_research.remoteanytarget.RemoteAnyTarget._exists',
        lambda *args, **kwargs: False
    )
    assert not task.complete()

    # Monkeypatch RemoteFileSystem.exists to return True. The task
    # should now be completed.
    monkeypatch.setattr(
        'siptools_research.remoteanytarget.RemoteAnyTarget._exists',
        lambda *args, **kwargs: True
    )
    assert task.complete()
