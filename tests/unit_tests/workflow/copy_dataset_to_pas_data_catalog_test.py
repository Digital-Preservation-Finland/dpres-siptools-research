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
def test_copy_ida_dataset(config, workspace, requests_mock):
    """Test copying Ida dataset to PAS data catalog.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
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

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=original_version_id,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # Preservation state should be set
    assert mock_preservation.called_once
    assert mock_preservation.last_request.json() == {
        "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        "description": {"en": "Packaging dataset"}
    }
    # The PAS datacatalog version should be created
    assert mock_create_preservation_version.called_once


@pytest.mark.usefixtures('testmongoclient')
def test_ida_dataset_already_copied(config, workspace, requests_mock):
    """Test running task when dataset has already been copied.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    dataset["preservation"]["state"] = DS_STATE_INITIALIZED
    dataset["preservation"]["dataset_version"] = {
        "id": "pas-version-id",
        "persistent_identifier": None,
        "preservation_state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    }
    get_dataset = requests_mock.get(f"/v3/datasets/{workspace.name}",
                                    json=dataset)

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # Only one get request should be sent. No PATCHing or POSTing.
    assert len(requests_mock.request_history) == 1
    assert get_dataset.called_once


@pytest.mark.usefixtures('testmongoclient')
def test_copy_pas_dataset(config, workspace, requests_mock):
    """Test running task for pottumounttu dataset.

    The dataset was originally created in PAS catalog. So it is not
    actually copied anywhere, but preservation state is updated.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    original_version_id = workspace.name
    dataset["id"] = original_version_id
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["preservation"]["state"] = DS_STATE_METADATA_CONFIRMED
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    metax_mock = requests_mock.patch(
        f"/v3/datasets/{original_version_id}/preservation"
    )

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.last_request.json() == {
        "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        "description": {"en": "Packaging dataset"}
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
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["preservation"]["state"] \
        = DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    metax_mock = requests_mock.patch(
        "/v3/datasets/original-version-id/preservation"
    )

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called
