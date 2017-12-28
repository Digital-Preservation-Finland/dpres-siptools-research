"""Test the `siptools_research.workflow.report_preservation_status` module"""

import os
import time
import paramiko
from siptools_research.workflow import report_preservation_status


def test_reportpreservationstatus(testpath, testmongoclient, testmetax):
    """Creates new directory to "accepted" directory in digital preservation
    server, runs ReportPreservationStatus task, and tests that task is complete
    after it has been run. Fake Metax server is used, so it can not be tested
    if preservation status really is updtated in Metax."""

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
        workspace_name = os.path.basename(workspace)
        ssh.exec_command("mkdir -p accepted/%s/%s" % (datedir, workspace_name))

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        workspace=workspace,
        dataset_id="report_preservation_status_test_dataset_1",
        config='tests/data/siptools_research.conf'
    )
    assert not task.complete()
    task.run()
    assert task.complete()


# TODO: Test for case where SIP is rejected

# TODO: Check which requests were sent to httpretty
