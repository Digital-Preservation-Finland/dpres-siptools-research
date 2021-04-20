"""Integration test for :mod:`siptools_research.workflow.send_sip` module."""  # noqa: W505,E501

import logging
import os
import os.path
import shutil

import pytest
from siptools_research.workflow.send_sip import SendSIPToDP

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)


@pytest.mark.usefixtures('testmongoclient')
def test_send_sip(testpath, luigi_mock_ssh_config, sftp_dir):
    """Test the SendSipToDP task.

    Run task and check that .tar is copied to digital preservation
    server.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create workspace with directories and files required by the task
    workspace = testpath

    os.makedirs(os.path.join(workspace, 'sip-in-progress'))
    tar_file_name = os.path.basename(workspace) + ".tar"
    shutil.copy('tests/data/testsips/simple_sip.tar',
                os.path.join(workspace, tar_file_name))

    sftp_dir.mkdir("transfer")

    # Init and run task
    task = SendSIPToDP(workspace=workspace,
                       dataset_id='1',
                       config=luigi_mock_ssh_config)
    task.run()
    assert task.complete()

    # Check that tar-file is created on the SFTP server
    target_file_path = os.path.join(
        str(sftp_dir), "transfer", tar_file_name
    )
    assert os.path.isfile(target_file_path)
