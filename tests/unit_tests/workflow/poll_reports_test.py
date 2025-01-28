"""Unit tests for :mod:`siptools_research.workflow.validate_sip`."""
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from siptools_research.workflow.poll_reports import GetValidationReports
import tests.metax_data.datasetsV3
from tests.metax_data.datasets import BASE_DATASET


@pytest.mark.parametrize(
    'status',
    [
        ('rejected'),
        ('accepted')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_getvalidationreports(config, workspace, requests_mock, status):
    """Initializes GetValidationReports task with the input files of the task.

    After the input files are created, the GetValidationReports is
    triggered automatically. Checks that the ingest reports were
    succesfully loaded to the workspace and the task is completed.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :param status: SIP's status in DPS.
    """
    # Mock metax API V3
    doi = "doi:test"
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    dataset["persistent_identifier"] = doi
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)

    # Mock metax API V2
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    requests_mock.get(f"/rest/v2/datasets/{workspace.name}?include_user_metadata=true&file_details=true",
                      json = dataset)

    #Mock DPS
    requests_mock.get(
        "https://access.localhost/api/2.0/contract_identifier/ingest/report/doi%3Atest",
        json={
            "data": {
                "results": [
                    {
                        "download": {
                            "html": "foo?type=html",
                            "xml": "foo?type=xml"
                        },
                        "id": doi,
                        "date": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "status": status
                    }
                ]
            }
        }
    )

    requests_mock.get('https://access.localhost/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=xml',
                      content=b'<hello world/>')
    requests_mock.get('https://access.localhost/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=html',
                      content=b'<html>hello world</html>')

    # Init task
    task = GetValidationReports(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Task is run when the input file is created.
    file_content = f'Dataset id={dataset["identifier"]},{(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}'
    Path(task.input().path).write_text(file_content)
    assert task.complete()

    # Ingest reports appeared
    ingest_report_path \
        = workspace / "preservation" / "ingest-reports" / status
    assert (ingest_report_path / f"{doi}.xml").read_text() == "<hello world/>"
    assert (ingest_report_path / f"{doi}.html").read_text() \
        == "<html>hello world</html>"


@pytest.mark.usefixtures('testmongoclient')
def test_getvalidationreports_is_not_completed_if_ingest_reports_are_older_than_sip(
        config, workspace, requests_mock
):
    """If a SIP's is newer than the ingest report's,
        ingest reports are not loaded to workspace.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax API V3
    doi = "doi:test"
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    dataset["persistent_identifier"] = doi
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)

    # Mock Metax API V2
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    #Mock metax
    requests_mock.get(
        f"/rest/v2/datasets/{workspace.name}?include_user_metadata=true&file_details=true",
        json = dataset
    )

    #Mock DPS
    requests_mock.get(
        "https://access.localhost/api/2.0/contract_identifier/ingest/report/doi%3Atest",
        json={
            "data": {
                "results": [
                    {
                        "download": {
                            "html": "foo?type=html",
                            "xml": "foo?type=xml"
                        },
                        "id": doi,
                        "date": (datetime.now(timezone.utc)-timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "status": 'accepted'
                    }
                ]
            }
        }
    )

    task = GetValidationReports(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # This should complete the task but because DPS entries are older than SIP
    # task is not completed and the files are not created.
    file_content = f'Dataset id={dataset["identifier"]},{datetime.now(timezone.utc).isoformat()}'
    Path(task.input().path).write_text(file_content)
    assert not task.complete()

    ingest_report_path \
        = workspace / "preservation" / "ingest-reports"
    assert not (ingest_report_path / "accepted" / f"{doi}.xml").exists()
    assert not (ingest_report_path / "accepted" / f"{doi}.html").exists()
    assert not ingest_report_path.exists()
