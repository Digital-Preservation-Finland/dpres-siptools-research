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

    :param testpath: Temporary directory fixture
    :returns: ``None``
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

    :param testpath: Temporary directory fixture
    :returns: ``None``
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

    :param path: Path of new directory
    :returns: ``None``
    """
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            '86.50.168.218',
            username='cloud-user',
            key_filename=os.path.expanduser("~") + '/.ssh/pouta-key.pem'
        )
        parent_path = os.path.dirname(path.strip('/'))
        _remote_cmd(ssh, 'sudo mkdir /home/tpas/' + parent_path)
        _remote_cmd(ssh, 'sudo chown tpas:access-rest-api /home/'
                    'tpas/' + parent_path, raise_error=True)
        _remote_cmd(ssh, 'sudo chmod 0775 /home/tpas/' + parent_path,
                    raise_error=True)
        _remote_cmd(ssh, 'sudo mkdir /home/tpas/' + path, raise_error=True)


def _remote_cmd(ssh, command, raise_error=False):
    """Runs a command on remote host.

    :param ssh: SSHClient used for running the command
    :param command: Command to be run on remote host
    :param raise_error: When true raises an Exception if command fails
    :returns: ``None``
    """
    _, stdout, _ = ssh.exec_command(command)
    if raise_error and stdout.channel.recv_exit_status() != 0:
        raise Exception('Remote command: ' + command + ' failed')
