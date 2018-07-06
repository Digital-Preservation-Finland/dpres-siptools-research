"""Integration tests for digital preservation server and
`siptools_research.workflow.report_preservation_status` module"""

import os
import time
import pytest
import tests.conftest
import paramiko
from siptools_research.workflow import report_preservation_status
from siptools_research.workflowtask import InvalidDatasetError
import mock


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_reportpreservationstatus(testpath):
    """Creates new directory to "accepted" directory in digital preservation
    server, runs ReportPreservationStatus task, and tests that task is complete
    after it has been run. Fake Metax server is used, so it can not be tested
    if preservation status really is updated in Metax.

    :testpath: Temporary directory fixture
    :returns: None
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('86.50.168.218',
                    username='tpas',
                    key_filename='tests/data/pas_ssh_key')

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has completed.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        ssh.exec_command("mkdir -p accepted/%s/%s" % (datedir, tar_name))

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

    :testpath: Temporary directory fixture
    :returns: None
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('86.50.168.218',
                    username='tpas',
                    key_filename='tests/data/pas_ssh_key')

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has been rejected.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        dir_path = "rejected/%s/%s" % (datedir, tar_name)
        ssh.exec_command("mkdir -p " + dir_path)
        ssh.exec_command("touch " + dir_path + "/" +
                         os.path.basename(workspace) +
                         ".html")

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

    # Ingest report HTML should be copied to workspace
    assert os.path.isfile(workspace + '/' +
                          os.path.basename(workspace) + '.html')

    # The task should not be completed
    assert not task.complete()


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected_int_error(testpath):
    """Creates new directory to "rejected" directory with two report files
    in digital preservation server, runs ReportPreservationStatus task,
    and tests that the report file is NOT sent. Metax server is used, so it can
    not be tested if preservation status really is updated in Metax.

    :testpath: Temporary directory fixture
    :returns: None
    """

    workspace = testpath

    # Set permissions of ssh-key (required by paramiko)
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create new directory to digital preservation server
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('86.50.168.218',
                    username='tpas',
                    key_filename='tests/data/pas_ssh_key')

        # Create directory with name of the workspace to digital preservation
        # server over SSH, so that the ReportPreservationStatus thinks that
        # validation has been rejected.
        datedir = time.strftime("%Y-%m-%d")
        tar_name = os.path.basename(workspace) + '.tar'
        dir_path = "rejected/%s/%s" % (datedir, tar_name)
        ssh.exec_command("mkdir -p " + dir_path)
        ssh.exec_command("touch " + dir_path + "/" +
                         os.path.basename(workspace) +
                         ".html")
        ssh.exec_command("touch " + dir_path + "/" +
                         os.path.basename(workspace) +
                         "_extra.html")

    # Init and run task
    with mock.patch('siptools_research.workflow.'\
                    'report_preservation_status.mail.send') as mock_sendmail:
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
        except ValueError:
            exceptionThrown = True
        mock_sendmail.assert_not_called()
        assert exceptionThrown is True
        assert task.complete() is False


# TODO: Check which requests were sent to httpretty
