"""Tests for ``siptools_research.api.file_errors`` module."""
import pytest

from siptools_research.models.file_error import FileError


@pytest.fixture
def default_file_errors():
    """
    Default file errors to be added into the error database.

    Used in the test cases.
    """
    file_errors = []
    for i in range(0, 20):
        file_error = FileError(
            file_id=f"file-id-{i}",
            storage_identifier=f"storage-identifier-id-{i}",
            # Every file with even id is stored in IDA, odd files in PAS
            storage_service="ida" if i % 2 == 0 else "pas",
            # Every 5th file error has a dataset
            dataset_id=f"dataset-id-{i}" if i % 5 == 0 else None,
            errors=[f"error {i}"]
        )
        file_error.save()

        file_errors.append(file_error)

    return file_errors


@pytest.mark.usefixtures("default_file_errors")
def test_retrieve_file_errors(client):
    """
    Try retrieving two file errors and ensure they have the correct
    data
    """
    response = client.post(
        "/file-errors",
        data={"ids": "file-id-1,file-id-5"}
    )
    file_errors = response.json

    # Two files found
    assert len(response.json) == 2

    file_1 = next(
        file for file in file_errors
        if file["id"] == "file-id-1"
    )
    file_5 = next(
        file for file in file_errors
        if file["id"] == "file-id-5"
    )

    assert file_1["storage_service"] == "pas"
    assert file_1["storage_identifier"] == "storage-identifier-id-1"
    assert file_1["errors"] == ["error 1"]
    assert file_1["dataset_id"] is None

    assert file_5["storage_identifier"] == "storage-identifier-id-5"
    assert file_5["errors"] == ["error 5"]
    # Every fifth test file has an associated dataset
    assert file_5["dataset_id"] == "dataset-id-5"


@pytest.mark.usefixtures("default_file_errors")
@pytest.mark.parametrize(
    "params,expected_ids",
    [
        (
            {"ids": "file-id-1"},
            ["file-id-1"]
        ),
        (
            # Every odd test file is stored in PAS
            {"storage_service": "pas"},
            [
                "file-id-1", "file-id-3", "file-id-5", "file-id-7",
                "file-id-9", "file-id-11", "file-id-13", "file-id-15",
                "file-id-17", "file-id-19"
            ]
        ),
        (
            {
                "storage_identifiers": \
                    "storage-identifier-id-1,storage-identifier-id-5"
            },
            ["file-id-1", "file-id-5"]
        ),
        (
            {
                "dataset_id": "dataset-id-5"
            },
            ["file-id-5"]
        ),
        (
            # Query parameters result in an AND query
            {
                "ids": "file-id-0,file-id-1,file-id-2,file-id-3",
                "storage_service": "ida"
            },
            ["file-id-0", "file-id-2"]
        )
    ]
)
def test_file_error_filter(client, params, expected_ids):
    """
    Test retrieving file errors with given POST parameters.
    """
    response = client.post(
        "/file-errors",
        data=params
    )
    file_errors = response.json

    found_file_ids = {file["id"] for file in file_errors}

    assert set(expected_ids) == found_file_ids
