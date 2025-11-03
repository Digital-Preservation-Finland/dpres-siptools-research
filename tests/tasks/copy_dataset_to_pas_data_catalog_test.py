"""Unit tests for CopyToPasDataCatalog task."""
import copy

from metax_access import (
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_DATASET_VALIDATED,
)

from siptools_research.tasks import copy_dataset_to_pas_data_catalog
from metax_access.template_data import DATASET


def test_copy_ida_dataset(config, workspace, requests_mock):
    """Test copying Ida dataset to PAS data catalog.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    original_version_id = workspace.name
    dataset["id"] = original_version_id
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    dataset["preservation"]["state"] = DS_STATE_METADATA_CONFIRMED
    requests_mock.get(f"/v3/datasets/{original_version_id}", json=dataset)
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

    # The PAS datacatalog version should be created
    assert mock_create_preservation_version.called_once


def test_ida_dataset_already_copied(config, workspace, requests_mock):
    """Test running task when dataset has already been copied.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    dataset["preservation"]["state"] = DS_STATE_INITIALIZED
    dataset["preservation"]["dataset_version"] = {
        "id": "pas-version-id",
        "persistent_identifier": None,
        "preservation_state": DS_STATE_DATASET_VALIDATED,
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


def test_copy_pas_dataset(config, workspace, requests_mock):
    """Test running task for pottumounttu dataset.

    The dataset was originally created in PAS catalog. So nothing should
    be done.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
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

    assert not metax_mock.called


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
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = "original-version-id"
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["preservation"]["state"] = DS_STATE_DATASET_VALIDATED
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
