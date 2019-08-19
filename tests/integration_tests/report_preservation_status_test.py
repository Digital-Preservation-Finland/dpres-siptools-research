"""Integration tests for digital preservation server and
:mod:`siptools_research.workflow.report_preservation_status` module"""

import os
import time

import pytest
import paramiko
import mock

from metax_access import Metax

import tests.conftest
from tests.metax_data import datasets
from siptools_research.workflow import report_preservation_status
from siptools_research.workflowtask import InvalidDatasetError


@pytest.fixture(autouse=True)
def mock_metax_access(monkeypatch):
    """Mock metax_access GET requests to files or datasets to return
    mock functions from metax_data.datasets and metax_data.files modules.
    """
    monkeypatch.setattr(
        Metax, "set_preservation_state",
        lambda self, dataset_id, **kwargs: datasets.get_dataset("", dataset_id)
    )


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_reportpreservationstatus(testpath):
    """Creates new directory to "accepted" directory in digital preservation
    server, runs ReportPreservationStatus task, and tests that task is complete
    after it has been run. Fake Metax server is used, so it can not be tested
    if preservation status really is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)
    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            '86.50.168.218',
            username='cloud-user',
            key_filename=os.path.expanduser("~") + '/.ssh/pouta-key.pem'
        )
        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has completed.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        _remote_cmd(ssh, "sudo mkdir -p /home/tpas/accepted/%s/%s" %
                    (datedir, tar_name))
        _remote_cmd(ssh, "sudo chown tpas:access-rest-api "
                    "/home/tpas/accepted/%s/%s" % (datedir, tar_name),
                    raise_error=True)

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        workspace=workspace,
        dataset_id="report_preservation_status_test_dataset_ok",
        config=tests.conftest.TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected(testpath):
    """Creates new directory with a report file to "rejected" directory in
    digital preservation server. Runs ReportPreservationStatus task, which
    should raise an exception and write ingest report HTML to workspace. Fake
    Metax server is used, so it can not be tested if preservation status really
    is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            '86.50.168.218',
            username='cloud-user',
            key_filename=os.path.expanduser("~") + '/.ssh/pouta-key.pem'
        )

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has been rejected.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        dir_path = "rejected/%s/%s" % (datedir, tar_name)
        _remote_cmd(ssh, "sudo mkdir -p /home/tpas/" + dir_path)
        _remote_cmd(ssh, "sudo chown tpas:access-rest-api /home/tpas/" +
                    dir_path)
        _remote_cmd(ssh, "sudo touch /home/tpas/" + dir_path + "/" +
                    os.path.basename(workspace) + ".html")
        _remote_cmd(ssh, "sudo chown tpas:access-rest-api /home/tpas/" +
                    dir_path + "/" + os.path.basename(workspace) + ".html",
                    raise_error=True)
    # Init task
    task = report_preservation_status.ReportPreservationStatus(
        workspace=workspace,
        dataset_id="report_preservation_status_test_dataset_rejected",
        config=tests.conftest.TEST_CONFIG_FILE
    )

    # Running task should raise exception
    with pytest.raises(InvalidDatasetError) as exc_info:
        task.run()
    assert exc_info.value[0] == "SIP was rejected"

    # The task should not be completed
    assert not task.complete()


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected_int_error(testpath):
    """Creates new directory to "rejected" directory with two report files
    in digital preservation server, runs ReportPreservationStatus task,
    and tests that the report file is NOT sent. Metax server is used, so it can
    not be tested if preservation status really is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            '86.50.168.218',
            username='cloud-user',
            key_filename=os.path.expanduser("~") + '/.ssh/pouta-key.pem'
        )

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has been rejected.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        dir_path = "rejected/%s/%s" % (datedir, tar_name)
        _remote_cmd(ssh, 'sudo mkdir -p /home/tpas/' + dir_path)
        _remote_cmd(ssh, 'sudo chown tpas:access-rest-api /home/'
                    'tpas/' + dir_path, raise_error=True)
        _remote_cmd(ssh, "sudo touch /home/tpas/" + dir_path + "/" +
                    os.path.basename(workspace) + ".html")
        _remote_cmd(ssh, "sudo chown tpas:access-rest-api /home/tpas/" +
                    dir_path + "/" + os.path.basename(workspace) + ".html",
                    raise_error=True)
        _remote_cmd(ssh, "sudo touch /home/tpas/" + dir_path + "/" +
                    os.path.basename(workspace) + "_extra.html")
        _remote_cmd(ssh, "sudo chown tpas:access-rest-api /home/tpas/" +
                    dir_path + "/" + os.path.basename(workspace) +
                    "_extra.html", raise_error=True)

    # Run task like it would be run from command line
    exceptionThrown = False
    task = report_preservation_status.ReportPreservationStatus(
        workspace=workspace,
        dataset_id="report_preservation_status_test_dataset_rejected",
        config=tests.conftest.TEST_CONFIG_FILE
    )
    assert not task.complete()
    try:
        task.run()
    except (ValueError, InvalidDatasetError):
        exception_thrown = True

    assert exception_thrown
    assert task.complete() is False


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

# TODO: Check which requests were sent to httpretty
