"""Unit tests for :mod:`siptools_research.workflow.send_sip` module."""
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from siptools_research.workflow.send_sip import SendSIPToDP


@pytest.mark.usefixtures('testmongoclient')
def test_send_sip(workspace, mock_ssh_config, sftp_dir):
    """Test the SendSipToDP task.

    Run task and check that .tar is copied to digital preservation
    server.

    :param workspace: Temporary workspace directory
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
    task = SendSIPToDP(dataset_id=workspace.name, config=str(mock_ssh_config))
    task.run()
    assert task.complete()

    # Check that tar-file is created on the SFTP server
    assert (transfer_dir / tar_file_name).is_file()

    # Check that a timestamp exists and has at least todays date
    log_file_path \
        = Path(workspace / "preservation" / "task-send-sip-to-dp.finished")
    sip_to_dp_str = log_file_path.read_text().split(',')[-1]
    assert sip_to_dp_str
    sip_to_dp_date = datetime.fromisoformat(sip_to_dp_str)
    assert sip_to_dp_date.date() == datetime.now(timezone.utc).date()

