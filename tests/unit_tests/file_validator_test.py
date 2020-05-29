"""Tests for :mod:`siptools_research.file_validator` module"""

import pytest

from siptools_research.file_validator import (
    validate_files,
    FileValidationError
)
from siptools_research.utils.download import FileAccessError

import tests.conftest


@pytest.mark.usefixtures("mock_metax_access", "testpath")
def test_validate_files(requests_mock):
    """Test that validate_metadata function returns ``True`` for valid files.

    :param requests_mock: Mocker object
    """
    requests_mock.patch(
        'https://metaksi/rest/v1/datasets/validate_files_valid'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:textfile1/download',
        content=b'foo'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:textfile2/download',
        content=b'bar'
    )
    assert validate_files(
        "validate_files_valid",
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_description'] \
        == 'Files passed validation'
    assert last_request['preservation_state'] == 70


@pytest.mark.usefixtures("mock_metax_access", "testpath")
def test_validate_invalid_files(requests_mock):
    """Test that validating files with wrong mimetype raises
    FileValidationError.

    :param requests_mock: Mocker object
    """
    requests_mock.patch(
        'https://metaksi/rest/v1/datasets/validate_files_invalid'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:invalid_mimetype_1/download'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:invalid_mimetype_2/download'
    )

    with pytest.raises(FileValidationError) as error:
        validate_files(
            "validate_files_invalid",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(error.value) == (
        "Following files are not well-formed:\n"
        "path/to/file\n"
        "path/to/file"
    )
    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_description']\
        .startswith("Following files")
    assert last_request['preservation_state'] == 40


@pytest.mark.usefixtures("mock_metax_access", "testpath")
def test_validate_files_not_found(requests_mock):
    """Test that validating files, which are not found.

    :param requests_mock: Mocker object
    """
    requests_mock.patch(
        'https://metaksi/rest/v1/datasets/validate_files_not_found'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:not_found_1/download',
        status_code=404
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:not_found_2/download',
        status_code=404
    )

    with pytest.raises(FileAccessError) as error:
        validate_files(
            "validate_files_not_found",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    expected_error_message = "Could not download file 'path/to/file'"
    assert str(error.value) == expected_error_message

    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_state'] == 50
    assert last_request['preservation_description'].startswith(
        expected_error_message
    )
