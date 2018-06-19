"""Integration tests for digital preservation service and
``siptools_research.workflow.validate_sip`` module.
"""

import os
import time
import tests.conftest
import paramiko
from siptools_research.workflow.validate_sip import ValidateSIP

def test_validatesip_accepted(testpath):
    """Initializes validate_sip task and tests that it is not complete. Then
    creates new directory to "accepted" directory in digital preservation
    server and tests that task is complete.

    :testpath: Temporary directory fixture
    :returns: None
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=tests.conftest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    _create_remote_dir("accepted/%s/%s" % (datedir, tar_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def test_validatesip_rejected(testpath):
    """Initializes validate-sip task and tests that it is not complete. Then
    creates new directory to "rejected" directory in digital preservation
    server and tests that task is complete.

    :testpath: Temporary directory fixture
    :returns: None
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=tests.conftest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    _create_remote_dir("rejected/%s/%s" % (datedir, tar_name))

    # Check that task is completed after new directory is created
    assert task.complete()


def _create_remote_dir(path):
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
