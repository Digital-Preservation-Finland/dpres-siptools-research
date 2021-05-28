"""Tests for :mod:`siptools_research.file_validator` module."""

import pytest

from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from siptools_research.exceptions import MissingFileError

import tests.conftest


@pytest.mark.usefixtures("mock_metax_access", "pkg_root")
def test_validate_files(requests_mock):
    """Test that validate_metadata function returns ``True`` for valid files.

    :param requests_mock: Mocker object
    """
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


@pytest.mark.usefixtures("mock_metax_access", "pkg_root")
def test_validate_invalid_files(requests_mock):
    """Test validating files with wrong mimetype.

    Wrong mimetype should raise InvalidFileError.

    :param requests_mock: Mocker object
    """
    requests_mock.get(
        'https://ida.test/files/pid:urn:invalid_mimetype_1/download'
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:invalid_mimetype_2/download'
    )

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files(
            "validate_files_invalid",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "2 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:invalid_mimetype_1',
                                          'pid:urn:invalid_mimetype_2']


@pytest.mark.usefixtures("mock_metax_access", "pkg_root")
def test_validate_files_not_found(requests_mock):
    """Test that validating files, which are not found.

    :param requests_mock: Mocker object
    """
    requests_mock.get(
        'https://ida.test/files/pid:urn:not_found_1/download',
        status_code=404
    )
    requests_mock.get(
        'https://ida.test/files/pid:urn:not_found_2/download',
        status_code=404
    )

    with pytest.raises(MissingFileError) as exception_info:
        validate_files(
            "validate_files_not_found",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "2 files are missing"
    assert exception_info.value.files == ['pid:urn:not_found_1',
                                          'pid:urn:not_found_2']
