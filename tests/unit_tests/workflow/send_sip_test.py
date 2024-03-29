"""Unit tests for :mod:`siptools_research.workflow.send_sip` module."""

import logging
import shutil

import pytest
from siptools_research.workflow.send_sip import SendSIPToDP

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)


@pytest.mark.usefixtures('testmongoclient')
def test_send_sip(workspace, luigi_mock_ssh_config, sftp_dir):
    """Test the SendSipToDP task.

    Run task and check that .tar is copied to digital preservation
    server.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    tar_file_name = f"{workspace.name}.tar"
    shutil.copy(
        'tests/data/testsips/simple_sip.tar',
        workspace / "preservation" / tar_file_name
    )

    transfer_dir = sftp_dir / "transfer"
    transfer_dir.mkdir()

    # Init and run task
    task = SendSIPToDP(dataset_id=workspace.name,
                       config=luigi_mock_ssh_config)
    task.run()
    assert task.complete()

    # Check that tar-file is created on the SFTP server
    assert (transfer_dir / tar_file_name).is_file()
