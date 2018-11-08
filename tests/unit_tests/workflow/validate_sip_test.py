"""Units tests for ``siptools_research.workflow.validate_sip`` module."""


import tests.conftest
from siptools_research.workflow.validate_sip import ValidateSIP


# pylint: disable=unused-argument
def _always_true(*args, **kwargs):
    """Mock function that always returns True.

    :returns: True
    """
    return True


def _always_false(*args, **kwargs):
    """Mock function that always returns False.

    :returns: False
    """
    return False


def test_validatesip_accepted(testpath, monkeypatch):
    """Initializes validate_sip task and tests that it is not complete. Luigi
    code is then monkeypatched to always think that remote target exists. The
    task should then be completed.

    :testpath: Temporary directory fixture
    :monkeypatch: Monkeypatch fixture
    :returns: None
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Monkeypatch RemoteFileSystem.exists to return False. The task should not
    # be completed.
    monkeypatch.setattr('siptools_research.remoteanytarget.'
                        'RemoteAnyTarget._exists',
                        _always_false)
    assert not task.complete()

    # Monkeypatch RemoteFileSystem.exists to return True. The task should now
    # be completed.
    monkeypatch.setattr('siptools_research.remoteanytarget.'
                        'RemoteAnyTarget._exists',
                        _always_true)
    assert task.complete()
