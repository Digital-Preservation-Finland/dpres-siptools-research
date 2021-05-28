"""Tests for :mod:`siptools_research.workflow.validate_metadata`."""

from pathlib import Path

import pytest
import tests.conftest
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata


@pytest.mark.usefixtures('testmongoclient', 'mock_filetype_conf',
                         'mock_metax_access')
def test_validatemetadata(workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has valid metadata.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get(
        'https://metaksi/rest/v1/contracts/contract_identifier',
        json={
            "contract_json": {
                "title": "Testisopimus",
                "identifier": "contract_identifier",
                "organization": {
                    "name": "Testiorganisaatio"
                }
            }
        }
    )
    requests_mock.get(
        'https://metaksi/rest/v1/directories/pid:urn:dir:wf1',
        json={
            "identifier": "pid:urn:dir:wf1",
            "directory_path": "/access"
        }
    )
    # Fake text files don't have technical metadata
    requests_mock.get(
        'https://metaksi/rest/v1/files/pid:urn:textfile1/xml',
        json=[]
    )
    requests_mock.get(
        'https://metaksi/rest/v1/files/pid:urn:textfile2/xml',
        json=[]
    )

    requests_mock.get(
        'https://metaksi/rest/v1/datasets/validate_metadata_test_dataset'
        '?dataset_format=datacite&dummy_doi=false',
        content=Path("./tests/data/datacite_sample.xml").resolve().read_bytes()
    )

    # Init task
    task = ValidateMetadata(workspace=str(workspace),
                            dataset_id='validate_metadata_test_dataset',
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()

    assert task.complete()


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_invalid_metadata(workspace):
    """Test ValidateMetadata class.

    Run task for dataset that has invalid metadata. The dataset is
    missing attribute: 'type' for each object in files list.

    :param workspace: Temporary workspace directory fixture
    :returns: ``None``
    """
    # Init task
    task = ValidateMetadata(
        workspace=str(workspace),
        dataset_id='validate_metadata_test_dataset_invalid_metadata',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidDatasetMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "'contract' is a required property" in str(exc.value)
    assert not task.complete()
