"""Test ``siptools_research.workflow.send_sip`` module"""
import os
import shutil
import logging
import pytest
import paramiko
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.config import Configuration

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)

@pytest.mark.usefixtures('testmongoclient')
def test_send_sip(testpath):
    """Test the workflow task SendSip module. Run task and check that .tar is
    copied to digital preservation server.

    :testpath: Temporary directory fixture
    :returns: None
    """
    # Set permissions of ssh key
    os.chmod('tests/data/pas_ssh_key', 0600)

    # Create workspace with directories and files required by the task
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))
    os.makedirs(os.path.join(workspace, 'logs'))
    tar_file_name = os.path.basename(workspace) + ".tar"
    shutil.copy('tests/data/testsips/simple_sip.tar',
                os.path.join(workspace, tar_file_name))

    # Init and run task
    task = SendSIPToDP(workspace=workspace,
                       dataset_id='1',
                       config='tests/data/siptools_research.conf')
    task.run()
    assert task.complete()

    # Init sftp connection to digital preservation server
    conf = Configuration('tests/data/siptools_research.conf')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(conf.get('dp_host'),
                username=conf.get('dp_user'),
                key_filename=conf.get('dp_ssh_key'))
    sftp = ssh.open_sftp()

    # Check that tar-file is created on remote host.
    # NOTE: Tar is copied to ~/transfer/. From there it is automatically moved
    # to /var/spool/preservation/ and after validation it is moved to
    # ~/rejected/<datedir>/<workspace>.tar/. There is a risk that file is moved
    # from ~/transfer before this test is finished.
    target_file_path = "/home/tpas/transfer/" + tar_file_name
    logging.debug('Looking for file: %s on server: %s',
                  target_file_path, conf.get('dp_host'))
    sftp.stat(target_file_path)
