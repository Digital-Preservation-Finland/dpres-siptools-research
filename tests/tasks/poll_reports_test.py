"""Unit tests for :mod:`siptools_research.tasks.validate_sip`."""
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from siptools_research.tasks.poll_reports import GetValidationReports
from metax_access.template_data import DATASET
from tests.utils import add_dps_transfer_mock


@pytest.mark.parametrize(
    'status',
    [
        ('rejected'),
        ('accepted')
    ]
)
@pytest.mark.parametrize("api_version", [2, 3])
def test_getvalidationreports(
        config, workspace, requests_mock, status, api_version):
    """Initializes GetValidationReports task with the input files of the task.

    After the input files are created, the GetValidationReports is
    triggered automatically. Checks that the transfer status was
    resolved and written to the output file, and that the task is
    completed.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :param status: SIP's status in DPS.
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    dataset["persistent_identifier"] = "doi:test"
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    contract_id = dataset['preservation']['contract']

    # Mock DPS
    if api_version == 3:
        transfer_id = "simple_sip.tar-00000000-0000-0000-0000-000000000001"
        add_dps_transfer_mock(
            requests_mock,
            contract_id=contract_id,
            transfer_id=transfer_id,
            status=status,
        )
    elif api_version == 2:
        requests_mock.get(
            f"https://access.localhost/api/2.0/{contract_id}/ingest/"
            "report/doi%3Atest",
            json={
                "data": {
                    "results": [
                        {
                            "download": {
                                "html": "foo?type=html",
                                "xml": "foo?type=xml"
                            },
                            "id": "doi:test",
                            "date": datetime.now(timezone.utc).strftime(
                                '%Y-%m-%dT%H:%M:%SZ'
                            ),
                            "status": status
                        }
                    ]
                }
            }
        )

    # Init task
    task = GetValidationReports(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Task is run when the input file is created.
    if api_version == 3:
        Path(task.input().path).write_text(transfer_id)
    elif api_version == 2:
        Path(task.input().path).write_text(
            f"Dataset id={dataset['id']},"
            f"{(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}"
        )
    assert task.complete()

    # The resolved status was written to the output file
    assert Path(task.output().path).read_text() == status


def test_getvalidationreports_not_completed_while_in_progress(
        config, workspace, requests_mock
):
    """If a transfer has not been processed yet, the task should not
    be completed and no output file should be created.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    contract_id = dataset['preservation']['contract']

    # Mock DPS
    transfer_id = "simple_sip.tar-00000000-0000-0000-0000-000000000001"
    add_dps_transfer_mock(
        requests_mock,
        contract_id=contract_id,
        transfer_id=transfer_id,
        status="in_progress",
    )

    task = GetValidationReports(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # This should complete the task but because the transfer is still
    # being processed in DPS, task is not completed and the output
    # file is not created.
    Path(task.input().path).write_text(transfer_id)
    assert not task.complete()
    assert not Path(task.output().path).exists()
