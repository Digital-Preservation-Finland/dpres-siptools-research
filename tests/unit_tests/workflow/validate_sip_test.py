"""Test task for testing ``siptools_research.workflow.validate_sip``."""
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
import pytest
import paramiko
from siptools_research.workflow.validate_sip import ValidateSIP

def test_validatesip_accepted(testpath):
    """Initializes task and tests that it is not complete. Then creates new
    directory to "accepted" directory in digital preservation server and tests
    that task is complete.

    :testpath: Temporary directory fixture
    :returns: None
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=pytest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    create_remote_dir("accepted/%s/%s" % (datedir, tar_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def test_validatesip_rejected(testpath):
    """Initializes task and tests that it is not complete. Then creates new
    directory to "rejected" directory in digital preservation server and tests
    that task is complete.

    :testpath: Temporary directory fixture
    :returns: None
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=pytest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    create_remote_dir("rejected/%s/%s" % (datedir, tar_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def create_remote_dir(path):
    """Creates new directory to digital preservation server and sets
    permissions of the new directory and its parent directory.

    :path: Path of new directory
    :returns: None
    """
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('86.50.168.218',
                    username='tpas',
                    key_filename='tests/data/pas_ssh_key')

        sftp = ssh.open_sftp()

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has completed.

        # Create parent directory
        parent_path = os.path.dirname(path.strip('/'))
        try:
            sftp.mkdir(parent_path)
        except IOError as exc:
            if exc.message == 'Failure':
                # The directory probably exists already
                pass
            else:
                raise
        sftp.chown(parent_path, 9906, 333) # tpas:preservation
        sftp.chmod(parent_path, 0775)

        # Create new directory
        sftp.mkdir(path)
        sftp.chown(path, 9906, 333) # tpas:preservation
        sftp.chmod(path, 0775)

