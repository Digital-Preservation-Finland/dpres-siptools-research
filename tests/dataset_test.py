"""Tests for :mod:`siptools_research.dataset` module."""
import copy
import datetime

import pytest
from metax_access.template_data import DATASET, FILE

from siptools_research.dataset import Dataset
from siptools_research.models.file_error import FileError


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            # Pottumonttu datasets are created in PAS data-catalog, so
            # there is only one version of pottumonttu dataset.
            # Therefore, the persistent identifier is the DOI which
            # should be used as SIP identifier. NOTE: In future there
            # will be separate data-catalog for pottumonttu datasets:
            # TPASPKT-749
            "persistent_identifier": "correct-id"
        },
        # Ida dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": -1,
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": "pas-version-uuid",
                    # This is the DOI of the PAS version of the
                    # dataset, which should, i.e. the identitier of
                    # the SIP
                    "persistent_identifier": "correct-id",
                    "preservation_state": -1
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            }
        },
    ]
)
def test_sip_identifier(config, requests_mock, metadata):
    """Test that dataset returns correct sip_identifier.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    dataset = Dataset(dataset['id'], config=config)
    assert dataset.sip_identifier == "correct-id"


def test_no_sip_identifier(config, requests_mock):
    """Test that exception is raised if dataset does not have SIP ID.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)
    dataset = Dataset(dataset['id'], config=config)
    with pytest.raises(ValueError, match='DOI does not exist'):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


def test_unknown_data_catalog(config, requests_mock):
    """Test that exception is raised if data catalog is unknown.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-unknown"
    requests_mock.get("/v3/datasets/identifier", json=dataset)

    dataset = Dataset("identifier", config=config)
    with pytest.raises(ValueError, match="Unknown data catalog"):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            "preservation": {
                "state": "correct-state",
                "description": None,
                "reason_description": None,
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": "correct-state",
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": None,
                    "persistent_identifier": None,
                    "preservation_state": -1,
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": "wrong-state",
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": "pas-version-id",
                    "persistent_identifier": "pas-version-doi",
                    "preservation_state": "correct-state",
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
    ]
)
def test_preservation_state(config, requests_mock, metadata):
    """Test that dataset returns correct preservation state.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get('/v3/datasets/identifier', json=dataset)

    dataset = Dataset('identifier', config=config)
    assert dataset.preservation_state == 'correct-state'


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            "id": "correct-id",
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "id": "correct-id",
            "preservation": {
                "dataset_version": None,
                "state": 0,
                "description": None,
                "reason_description": None,
                "contract": None,
                "pas_package_created": None,
            }
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "id": "wrong-id",
            "preservation": {
                "dataset_version": {
                    "id": "correct-id",
                    "persistent_identifier": None,
                    "preservation_state": 0
                },
                "state": 0,
                "description": None,
                "reason_description": None,
                "contract": None,
                "pas_package_created": None,
            }
        },
    ]
)
def test_set_preservation_state(config, requests_mock, metadata):
    """Test set_preservation_state method.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get("/v3/datasets/identifier", json=dataset)
    mocked_patch = requests_mock.patch("/v3/datasets/correct-id/preservation")

    dataset = Dataset("identifier", config=config)
    dataset.set_preservation_state("foo", "bar")

    # Check that the request contains correct message
    assert mocked_patch.called_once
    assert mocked_patch.last_request.json() == {
        "state": "foo",
        "description": {"en": "bar"}
    }


def test_task_log(config):
    """Test logging tasks and reading the log.

    :param config: Configuration file
    """
    dataset = Dataset('foo', config=config)

    # Add a task to log
    dataset.log_task('TestTask', 'success',
                     'Everything went better than expected')

    # Check that task was added to task log
    tasks = dataset._document.workflow_tasks
    assert tasks['TestTask']['messages'] \
        == 'Everything went better than expected'
    assert tasks['TestTask']['result'] == 'success'

    # Check that the timestamp of the task is correct format
    timestamp = dataset._document.workflow_tasks["TestTask"]["timestamp"]
    assert timestamp.endswith("+00:00")
    assert datetime.datetime.strptime(
        timestamp[:-6],  # Remove the UTC offset
        '%Y-%m-%dT%H:%M:%S.%f'
    )


def test_unlock_dataset(requests_mock, config):
    """Test unlocking dataset.

    Tests that `reset` updates Metax state, unlocks the dataset
    and removes any existing file errors

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )
    requests_mock.get(
        "/v3/datasets/test_dataset_id/files",
        json={
            "results": [
                FILE | {
                    "id": f"file-id-{i}",
                    "storage-identifier": f"storage-identifier-{i}"
                } for i in range(1, 3)  # Files 1 and 2
            ],
            "next": None,
            "previous": None
        }
    )
    files_post = requests_mock.post("/v3/files/patch-many")

    # Add dataset error to database
    dataset = Dataset("test_dataset_id", config=config)
    dataset.add_error("Test error message")

    # Add file errors to database
    FileError.objects.insert([
        FileError(
            file_id="file-id-1",
            storage_identifier="storage-identifier-1",
            storage_service="pas",
            dataset_id=None
        ),
        FileError(
            file_id="file-id-2",
            storage_identifier="storage-identifier-2",
            storage_service="pas",
            dataset_id="test_dataset_id"
        ),
        # This one won't be deleted
        FileError(
            file_id="file-id-3",
            storage_identifier="storage-identifier-3",
            storage_service="pas",
        )
    ])

    dataset.unlock()

    # Dataset metadata should be unlocked in Metax
    assert preservation_patch.called_once
    assert preservation_patch.last_request.json()["pas_process_running"] \
        is False

    # Metadata of all files ot the dataset should be unlocked in Metax
    assert files_post.called_once
    assert files_post.last_request.json() == [
        {
            "id": "file-id-1",
            "pas_process_running": False,
        },
        {
            "id": "file-id-2",
            "pas_process_running": False,
        },
    ]

    # Dataset errors should be cleared
    assert dataset.errors == []

    # File errors for the two files are removed
    assert FileError.objects.count() == 1
    assert FileError.objects.all()[0].file_id == "file-id-3"
