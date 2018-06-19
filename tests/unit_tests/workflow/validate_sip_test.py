"""Units tests for ``siptools_research.workflow.validate_sip`` module."""
# Automatic unit testing is complicated, because luigi RemoteTarget is using
# ssh command to connect remote server [1]. A temporary SSH-server should be
# created to properly test this module. MockSSH [2] library could do this. It
# allows definition of output for each command run on remote server (i.e.
# command: "test -e ~/accepted/..." would return "0" in this case). The problem
# with MockSSH is that it does not support authentication using SSH keys, so it
# always prompts for password [3]. The tests in this file are implemented by
# monkeypatching luigi code, so they do not truly test the functionality of
# validate-sip task.
#
#[1] http://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ssh.html
#[2] https://github.com/ncouture/MockSSH
#[3] https://github.com/ncouture/MockSSH/issues/14


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
                       config=tests.conftest.TEST_CONFIG_FILE)

    # Monkeypatch RemoteFileSystem.exists to return False. The task should not
    # be completed.
    monkeypatch.setattr('luigi.contrib.ssh.RemoteFileSystem.exists',
                        _always_false)
    assert not task.complete()

    # Monkeypatch RemoteFileSystem.exists to return True. The task should now
    # be completed.
    monkeypatch.setattr('luigi.contrib.ssh.RemoteFileSystem.exists',
                        _always_true)
    assert task.complete()
