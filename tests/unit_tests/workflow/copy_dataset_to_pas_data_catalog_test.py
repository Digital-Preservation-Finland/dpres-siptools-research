"""Unit tests for CopyToPasDataCatalog task."""
import copy

import pytest
from metax_access import (
    DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
)

from siptools_research.workflow import copy_dataset_to_pas_data_catalog
from tests.metax_data.datasetsV3 import BASE_DATASET


@pytest.mark.usefixtures('testmongoclient')
def test_copy_ida_dataset(config, workspace, requests_mock, request):
    """Test copying Ida dataset to PAS data catalog.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    :param request: Pytest CLI arguments
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(BASE_DATASET)
    original_version_id = workspace.name
    dataset["id"] = original_version_id
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    dataset["preservation"]["state"] = DS_STATE_METADATA_CONFIRMED
    requests_mock.get(f"/v3/datasets/{original_version_id}", json=dataset)
    mock_preservation = requests_mock.patch(
        f"/v3/datasets/{original_version_id}/preservation"
    )
    mock_create_preservation_version = requests_mock.post(
        f"/v3/datasets/{original_version_id}/create-preservation-version"
    )

    # Mock Metax API V2
    requests_mock.get(
        f'/rest/v2/datasets/{original_version_id}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-ida"
            },
            'identifier': original_version_id,
            'preservation_state': DS_STATE_METADATA_CONFIRMED,
            "research_dataset": {
                    "files": [
                        {
                            "details": {
                                "project_identifier": "foo"
                            }
                        }
                    ]
                }
        }
    )
    metax_v2_mock = requests_mock.patch(f'/rest/v2/datasets/{original_version_id}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=original_version_id,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    if request.config.getoption("--v3"):
        # Preservation state should be set
        assert mock_preservation.called_once
        assert mock_preservation.last_request.json() == {
            "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
            "description": {"en": "Packaging dataset"}
        }
        # The PAS datacatalog version should be created
        assert mock_create_preservation_version.called_once
    else:
        assert metax_v2_mock.called_once
        assert metax_v2_mock.last_request.json() == {
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
            'preservation_description':
            'Packaging dataset'
        }


@pytest.mark.usefixtures('testmongoclient')
def test_ida_dataset_already_copied(config, workspace, requests_mock):
    """Test running task when dataset has already been copied.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    dataset["preservation"]["state"] = DS_STATE_INITIALIZED
    dataset["preservation"]["dataset_version"] = {
        "id": "pas-version-id",
        "persistent_identifier": None,
        "preservation_state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    }
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    metax_mock = requests_mock.patch(
        "/v3/datasets/original-version-id/preservation"
    )

    # Mock Metax V2
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-ida"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_dataset_version': {
                'preferred_identifier': 'pas-version-id',
                'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            },
            'preservation_state': DS_STATE_INITIALIZED
        }
    )
    metax_v2_mock = requests_mock.patch(f'/rest/v2/datasets/{workspace.name}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called
    assert not metax_v2_mock.called


@pytest.mark.usefixtures('testmongoclient')
def test_copy_pas_dataset(config, workspace, requests_mock, request):
    """Test running task for pottumounttu dataset.

    The dataset was originally created in PAS catalog. So it is not
    actually copied anywhere, but preservation state is updated.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    :param request: Pytest CLI arguments
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(BASE_DATASET)
    original_version_id = workspace.name
    dataset["id"] = original_version_id
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["preservation"]["state"] = DS_STATE_METADATA_CONFIRMED
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    metax_mock = requests_mock.patch(
        f"/v3/datasets/{original_version_id}/preservation"
    )

    # Mock Metax V2
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'identifier': original_version_id,
            'preservation_state': DS_STATE_METADATA_CONFIRMED,
            "research_dataset": {
                    "files": [
                        {
                            "details": {
                                "project_identifier": "foo"
                            }
                        }
                    ]
                }
        }
    )
    metax_v2_mock = requests_mock.patch(f'/rest/v2/datasets/{original_version_id}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    if request.config.getoption("--v3"):
        assert metax_mock.last_request.json() == {
            "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
            "description": {"en": "Packaging dataset"}
        }
    else:
        assert metax_v2_mock.last_request.json() == {
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
            'preservation_description':
            'Packaging dataset'
        }


@pytest.mark.usefixtures('testmongoclient')
def test_pas_dataset_already_copied(config, workspace, requests_mock):
    """Test copying PAS dataset to PAS data catalog.

    The dataset was originally created in PAS catalog, and the
    preservation state has already been updated, so the
    CopyToPasDataCatalog task does not really do anything.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["preservation"]["state"] \
        = DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    metax_mock = requests_mock.patch(
        "/v3/datasets/original-version-id/preservation"
    )

    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
        }
    )
    metax_v2_mock = requests_mock.patch(f'/rest/v2/datasets/{workspace.name}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called
    assert not metax_v2_mock.called
