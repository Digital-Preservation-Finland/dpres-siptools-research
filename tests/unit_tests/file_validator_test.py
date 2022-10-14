"""Tests for :mod:`siptools_research.file_validator` module."""

import copy
import pytest

from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from siptools_research.exceptions import MissingFileError

from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TXT_FILE, TIFF_FILE
from tests.utils import add_mock_ida_download, add_metax_dataset

import tests.conftest


@pytest.mark.usefixtures("pkg_root")
def test_validate_files(requests_mock):
    """Test that validate_metadata function returns ``True`` for valid files.

    :param requests_mock: Mocker object
    """
    # Mock metax
    file1 = copy.deepcopy(TXT_FILE)
    file1['identifier'] = 'pid:urn:textfile1'
    file1['file_path'] = '/path/to/file1'
    # TODO: At the moment, the file validator does not validate checksum
    # of the file. When checksum validation is implemented, correct
    # checksum should be defined here.
    # file1['checksum']['value'] = '58284d6cdd8deaffe082d063580a9df3'
    file2 = copy.deepcopy(TXT_FILE)
    file2['identifier'] = 'pid:urn:textfile2'
    file2['file_path'] = '/path/to/file2'
    # file2['checksum']['value'] = '90dd1fc82b5d523f6f85716c1c67c0f3'
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[file1, file2]
    )

    # Mock Ida
    ida_mock1 = add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename="/path/to/file1",
        content=b"foo"
    )
    ida_mock2 = add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename="/path/to/file2",
        content=b"bar"
    )
    assert validate_files("dataset_identifier",
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Both files should have been downloaded
    assert ida_mock1.called
    assert ida_mock2.called


@pytest.mark.usefixtures("pkg_root")
def test_validate_invalid_files(requests_mock):
    """Test validating a not well-formed file.

    Try to validate an empty text file. File validator should detect the
    file as not well-formed and raise exception.

    :param requests_mock: Mocker object
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is text/plain.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TXT_FILE)]
    )

    # Mock Ida. Create a empty file.
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename="path/to/file",
        content=b""
    )

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files(
            "dataset_identifier",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier']


@pytest.mark.usefixtures("pkg_root")
def test_validate_wrong_mimetype(requests_mock):
    """Test validating a text file as a TIFF file.

    File validator should detect wrong mimetype, and raise exception.

    :param requests_mock: Mocker object
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is image/tiff.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TIFF_FILE)]
    )

    # Mock Ida. Create a plain text file instead of a TIFF file.
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename="path/to/file.tiff",
        content=b"foo"
    )

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files(
            "dataset_identifier",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier_tiff']


@pytest.mark.usefixtures("pkg_root")
def test_validate_files_not_found(requests_mock):
    """Test that validating files, which are not found.

    :param requests_mock: Mocker object
    """
    # Mock metax. Create a dataset that contains one text file which
    # does not exist in Ida.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TXT_FILE)]
    )

    # Mock Ida. Do not add any files to Ida.
    requests_mock.post(
        'https://ida.dl-authorize.test/authorize',
        status_code=404
    )

    with pytest.raises(MissingFileError) as exception_info:
        validate_files(
            "dataset_identifier",
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "1 files are missing"
    assert exception_info.value.files == ['pid:urn:identifier']
