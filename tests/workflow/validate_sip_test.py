"""Test task for testing ``siptools_workflow.validate_sip``."""
# TODO: Automatic unit testing is complicated, because luigi RemoteTarget is using
# ssh command to connect remote server [1]. A temporary SSH-server should be
# created to properly test this module. MockSSH [2] library could do this. It
# allows definition of output for each command run on remote server (i.e.
# command: "test -e ~/accepted/..." would return "0" in this case). The problem
# with MockSSH is that it does not support authentication using SSH keys, so it
# always prompts for password [3].
#
#[1] http://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/ssh.html
#[2] https://github.com/ncouture/MockSSH
#[3] https://github.com/ncouture/MockSSH/issues/14

import os
import time
import paramiko
import pytest
from siptools_research.workflow.validate_sip import ValidateSIP

# pylint: disable=redefined-outer-name,unused-argument
def test_validatesip_accepted(testpath):
    """Initializes task and tests that it is not complete. Then creates new
    directory to "accepted" directory in digital preservation server and tests
    that task is complete."""
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    workspace_name = os.path.basename(workspace)
    create_remote_dir("accepted/%s/%s" % (datedir, workspace_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def test_validatesip_rejected(testpath):
    """Initializes task and tests that it is not complete. Then creates new
    directory to "rejected" directory in digital preservation server and tests
    that task is complete."""
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    workspace_name = os.path.basename(workspace)
    create_remote_dir("rejected/%s/%s" % (datedir, workspace_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def create_remote_dir(path):
    """Creates new directory to digital preservation server

    :path: Path of new directory
    :returns: None
    """
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('86.50.168.218',
                    username='tpas',
                    key_filename='tests/data/pas_ssh_key')

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has completed.
        ssh.exec_command("mkdir -p %s" % path)
