"""Tests for :mod:`siptools_research.file_validator` module"""
import os
import json

import pytest
import pymongo
import httpretty

from siptools_research.file_validator import (validate_files,
                                              FileValidationError)
from siptools_research.config import Configuration
import tests.conftest


@pytest.fixture(autouse=True)
# pylint: disable=unused-argument
def _init_mongo_client(testmongoclient):
    """Initializes mocked mongo collection upload.files"""
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get("mongodb_host"))
    files_col = mongoclient.upload.files

    files = [
        "pid:urn:invalid_mimetype_1_local",
        "pid:urn:invalid_mimetype_2_local",
        "pid:urn:wf_test_1a_local",
        "pid:urn:wf_test_1b_local",
    ]
    for _file in files:
        files_col.insert_one({
            "_id": _file,
            "file_path": os.path.abspath(
                "tests/httpretty_data/ida/%s" % _file
            )
        })


@pytest.mark.parametrize(
    "filestorage",
    ["ida", "local"],
    ids=["ida", "upload-rest-api"],
)
@pytest.mark.usefixtures("testmetax", "testida", "mock_metax_access",
                         "testpath")
def test_validate_files(filestorage):
    """Test that validate_metadata function returns ``True`` for valid files.
    """
    assert validate_files(
        "validate_files_valid_%s" % filestorage,
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )


@pytest.mark.parametrize(
    "filestorage",
    ["ida", "local"],
    ids=["ida", "upload-rest-api"],
)
@pytest.mark.usefixtures("testmetax", "testida", "mock_metax_access",
                         "testpath")
def test_validate_invalid_files(filestorage):
    """Test that validating files with wrong mimetype raises
    FileValidationError.
    """
    with pytest.raises(FileValidationError) as error:
        validate_files(
            "validate_files_invalid_%s" % filestorage,
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(error.value) == (
        "Following files are not well-formed:\n"
        "path/to/file\n"
        "path/to/file"
    )


@pytest.mark.parametrize(
    "filestorage",
    ["ida", "local"],
    ids=["ida", "upload-rest-api"],
)
@pytest.mark.usefixtures("testmetax", "testida", "mock_metax_access",
                         "testpath")
def test_validate_files_not_found(filestorage):
    """Test that validating files, which are not found.
    """
    with pytest.raises(FileValidationError) as error:
        validate_files(
            "validate_files_not_found_%s" % filestorage,
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    if filestorage == "local":
        message = "File 'path/to/file' not found in pre-ingest file storage"
    else:
        message = "Could not download file 'path/to/file' from IDA"

    assert str(error.value) == message


def _assert_metadata_validation_passed(body_as_json):
    assert body_as_json == {
        'preservation_description': 'Metadata passed validation',
        'preservation_state': 70
    }


def _assert_metadata_validation_failed(body_as_json, description):
    assert body_as_json['preservation_state'] == 40
    assert body_as_json['preservation_description'].startswith(description)
