"""Unit tests for :mod:`siptools_research.tasks.send_sip` module."""
import copy
import shutil
from pathlib import Path

from metax_access.template_data import DATASET

from siptools_research.tasks.send_sip import SendSIPToDP
from tests.utils import add_dps_upload_mock


def test_send_sip(config, workspace, requests_mock):
    """Test the SendSipToDP task.

    Run task and check that the SIP is uploaded to the digital
    preservation system, and that the returned transfer ID is stored
    in the output file.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: Mocker object
    """
    tar_file_name = f"{workspace.name}.tar"
    shutil.copy(
        'tests/data/testsips/simple_sip.tar',
        workspace / "preservation" / tar_file_name
    )

    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)

    # Mock DPS
    transfer_id = f"{tar_file_name}-00000000-0000-0000-0000-000000000001"
    add_dps_upload_mock(requests_mock, transfer_id)

    # Init and run task
    task = SendSIPToDP(dataset_id=workspace.name, config=config)
    task.run()
    assert task.complete()

    # Check that the transfer ID is stored in the output file
    log_file_path \
        = Path(workspace / "preservation" / "task-send-sip-to-dp.finished")
    assert log_file_path.read_text() == transfer_id
