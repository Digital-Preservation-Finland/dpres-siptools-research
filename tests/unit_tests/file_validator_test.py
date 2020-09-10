"""Tests for :mod:`siptools_research.file_validator` module."""

import pytest

from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from siptools_research.exceptions import MissingFileError

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

    # At first preservation-state should be patched to
    # 'validating metadata' (65)
    data = {
        "preservation_state": 65
    }
    assert requests_mock.request_history[0].method == "PATCH"
    assert requests_mock.request_history[0].json() == data

    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_description'] \
        == 'Files passed validation'
    assert last_request['preservation_state'] == 70


@pytest.mark.usefixtures("mock_metax_access", "testpath")
def test_validate_invalid_files(requests_mock):
    """Test validating files with wrong mimetype.

    Wrong mimetype should raise InvalidFileError.

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

    with pytest.raises(InvalidFileError) as error:
        validate_files(
            "validate_files_invalid",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(error.value) == ("2 files are not well-formed")
    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_description']\
        == ("2 files are not well-formed:\n"
            "pid:urn:invalid_mimetype_1\n"
            "pid:urn:invalid_mimetype_2")
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

    with pytest.raises(MissingFileError) as error:
        validate_files(
            "validate_files_not_found",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(error.value) == "2 files are missing"

    # verify preservation_state is set as last operation
    last_request = requests_mock.request_history[-1].json()
    assert last_request['preservation_state'] == 40
    assert last_request['preservation_description']\
        == ("2 files are missing:\n"
            "pid:urn:not_found_1\n"
            "pid:urn:not_found_2")
