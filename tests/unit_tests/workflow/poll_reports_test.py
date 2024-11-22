"""Unit tests for :mod:`siptools_research.workflow.validate_sip`."""
import copy

from pathlib import Path
import pytest
from datetime import datetime, timezone, timedelta

from siptools_research.workflow.poll_reports import GetValidationReports
from tests.metax_data.datasets import BASE_DATASET

@pytest.mark.parametrize(
    'status',
    [
        ('rejected'),
        ('accepted')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_validatesip(workspace, luigi_mock_ssh_config, requests_mock, status):
    """Initializes validate_sip task and tests that it is not complete. Then
    runs the validate_sip task and checks that the ingest reports were
    succesfully loaded to the workspace.

    :param workspace: Temporary directory fixture
    :param luigi_mock_ssh_config: Configurations object
    :param requests_mock: Mocker object
    :param status: SIP's status in DPS.
    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    #Mock metax
    requests_mock.get(f'https://metaksi/rest/v2/datasets/{workspace.name}?include_user_metadata=true&file_details=true',
                      json = dataset)

    #Mock DPS
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest',
                      json={
                          "data": {
                            "results": [
                            {
                                "download": {
                                "html": "foo?type=html",
                                "xml": "foo?type=xml"
                            },
                            "id": dataset['preservation_identifier'],
                            "date": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "status": status
                      }
                    ]
                }
            }
        )
    
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=xml',
                      content=b'<hello world/>')
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=html',
                      content=b'<html>hello world</html>')

    # Init task
    task = GetValidationReports(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    # Task is run when the input file is created.
    file_content = f'Dataset id={dataset["identifier"]},{(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}'
    Path(task.input().path).write_text(file_content)
    assert task.complete()

    # Ingest reports appeared
    ingest_report_path \
        = workspace / "validation" / "ingest-reports" / status
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.xml").read_text() == "<hello world/>"
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.html").read_text() == "<html>hello world</html>"


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_timestamp_error(workspace, luigi_mock_ssh_config, requests_mock):
    """If a SIP's  is newer than the ingest report's ,
        ingest reports are not loaded to workspace.

    :param workspace: Temporary directory fixture
    :param luigi_mock_ssh_config: Configurations object
    :param requests_mock: Mocker object
    :returns: ``None``
    """

    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    #Mock metax
    requests_mock.get(f'https://metaksi/rest/v2/datasets/{workspace.name}?include_user_metadata=true&file_details=true',
                      json = dataset)

    #Mock DPS
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest',
                      json={
                          "data": {
                            "results": [
                            {
                                "download": {
                                "html": "foo?type=html",
                                "xml": "foo?type=xml"
                            },
                            "id": dataset['preservation_identifier'],
                            "date": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "status": 'accepted'
                      }
                    ]
                }
            }
        )

    task = GetValidationReports(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    file_content = f'Dataset id={dataset["identifier"]},{(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()}'
    Path(task.input().path).write_text(file_content)
    assert task.complete()
    
    ingest_report_path \
        = workspace / "validation" / "ingest-reports" / 'accepted'
    assert not (ingest_report_path / f"{dataset['preservation_identifier']}.xml").exists()
    assert not (ingest_report_path / f"{dataset['preservation_identifier']}.html").exists()
