"""Test the `siptools_research.workflow.report_preservation_status` module"""

import os
import time
import paramiko
from siptools_research.workflow import report_preservation_status


def test_reportpreservationstatus(testpath, testmongoclient, testmetax,
                                  monkeypatch):
    """Creates new directory to "accepted" directory in digital preservation
    server, runs ReportPreservationStatus task, and tests that task is complete
    after it has been run. Fake Metax server is used, so it can not be tested
    if preservation status really is updtated in Metax."""

    workspace = testpath

    # Use test password file instead of real Metax password
    monkeypatch.setattr('siptools_research.utils.metax.PASSWORD_FILE',
                        'tests/data/test_password_file')

    # Force ValidateSIP task to use SSH key from different path
    os.chmod('tests/data/pas_ssh_key', 0600)
    monkeypatch.setattr('siptools_research.workflow.validate_sip.DP_SSH_KEY',
                        'tests/data/pas_ssh_key')

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
        dataset_id="1"
    )
    assert not task.complete()
    task.run()
    assert task.complete()


# TODO: Test for case where SIP is rejected
