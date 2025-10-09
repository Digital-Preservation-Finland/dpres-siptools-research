"""Tests for ``siptools_research.api.dataset`` module."""
from siptools_research.dataset import Dataset
import tests.utils


def test_dataset_preserve(client, config):
    """Test preserving dataset.

    :param client: Flask test client
    :param config: Configuration file
    """
    response = client.post("/dataset/1/preserve")
    assert response.status_code == 202
    assert response.json == {
        "dataset_id": "1",
        "status": "preserving"
    }

    dataset = Dataset("1", config=config)
    assert dataset.enabled is True
    assert dataset.target.value == "preservation"


def test_dataset_generate_metadata(client, config):
    """Test the generating metadata.

    :param client: Flask test client
    :param config: Configuration file
    """
    response = client.post("/dataset/1/generate-metadata")
    assert response.status_code == 202
    assert response.json == {
        "dataset_id": "1",
        "status": "generating metadata"
    }

    dataset = Dataset("1", config=config)
    assert dataset.enabled is True
    assert dataset.target.value == "metadata_generation"


def test_dataset_reset(client, requests_mock):
    """Test resetting the dataset.

    :param client: Flask test client
    :param requests_mock: HTTP Request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)
    patch_many = requests_mock.post("/v3/files/patch-many")

    response = client.post(
        "/dataset/test_dataset_id/reset",
        data={
            "description": "Reset by user",
            "reason_description": "File was incorrect"
        }
    )
    assert response.status_code == 200
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "status": "dataset has been reset"
    }

    # Dataset does not contain any files, so empty list of files should
    # be patched
    assert patch_many.called_once
    assert patch_many.last_request.json() == []


def test_validate_dataset(client, config):
    """Test validating files and metadata.

    :param client: Flask test client
    :param config: Configuration file
    """
    response = client.post("/dataset/1/validate")
    assert response.status_code == 202
    assert response.json == {"dataset_id": "1", "status": "validating dataset"}

    dataset = Dataset("1", config=config)
    assert dataset.enabled
    assert dataset.target.value == "validation"
