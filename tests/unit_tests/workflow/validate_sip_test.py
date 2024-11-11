"""Unit tests for :mod:`siptools_research.workflow.validate_sip`."""
import copy

from datetime import datetime

import pytest

from siptools_research.workflow.validate_sip import ValidateSIP
from tests.metax_data.datasets import BASE_DATASET


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_accepted(workspace, luigi_mock_ssh_config, requests_mock):
    """Initializes validate_sip task and tests that it is not complete. Then
    creates new directory to "accepted" directory in digital preservation
    server and tests that task is complete.

    :param workspace: Temporary directory fixture
    :returns: ``None``
    """
    # Init task
    task = ValidateSIP(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    #Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
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
                            "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "status": "accepted"
                      }
                    ]
                }
            }
        )
    
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=xml',
                      content=b'<hello world/>')
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=html',
                      content=b'<html>hello world</html>')

    assert not task.complete()
    task.run()
    assert task.complete()
    ingest_report_path \
        = workspace / "validation" / "ingest-reports" / "accepted"
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.xml").read_text() == "<hello world/>"
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.html").read_text() == "<html>hello world</html>"


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_rejected(workspace, luigi_mock_ssh_config, requests_mock):
    """Initializes validate-sip task and tests that it is not complete. Then
    creates new directory to "rejected" directory in digital preservation
    server and tests that task is complete.

    :param workspace: Temporary directory fixture
    :returns: ``None``
    """
    # Init task
    task = ValidateSIP(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    #Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
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
                            "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "status": "rejected"
                      }
                    ]
                }
            }
        )
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=xml',
                      content=b'<hello world/>')
    requests_mock.get('https://access/api/2.0/contract_identifier/ingest/report/doi%3Atest/doi%3Atest?type=html',
                      content=b'<html>hello world</html>')

    assert not task.complete()
    task.run()
    assert task.complete()
    ingest_report_path \
        = workspace / "validation" / "ingest-reports" / "rejected"
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.xml").read_text() == "<hello world/>"
    assert (ingest_report_path / f"{dataset['preservation_identifier']}.html").read_text() == "<html>hello world</html>"
