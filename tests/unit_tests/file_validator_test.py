"""Tests for :mod:`siptools_research.file_validator` module."""

import copy
import shutil

import pytest

import tests.conftest
from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import SEG_Y_FILE, TIFF_FILE, TXT_FILE
from tests.utils import add_metax_dataset


def test_validate_files(requests_mock, testpath):
    """Test validating valid files.

    Validator should not raise any exceptions.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
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

    # Create valid files in temporary directory
    path1 = testpath / "path/to/file1"
    path1.parent.mkdir(parents=True)
    path1.write_text("foo")
    path2 = testpath / "path/to/file2"
    path2.write_text("bar")

    # Validator should return `None`, and exceptions should not be
    # raised.
    assert validate_files("dataset_identifier",
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE) is None


def test_validate_invalid_files(requests_mock, testpath):
    """Test validating a not well-formed file.

    Try to validate an empty text file. File validator should detect the
    file as not well-formed and raise exception.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is text/plain.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TXT_FILE)]
    )

    # Create a empty file to temporary directory
    filepath = testpath / "path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text("")

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files(
            "dataset_identifier",
            testpath,
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier']


def test_validate_bitlevel_file(requests_mock, testpath):
    """Test validating a file only accepted for bit-level preservation.

    File validator should ignore the validation result in this case.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is application/x.fi-dpres.segy.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(SEG_Y_FILE)]
    )

    # Copy a SEG-Y file to temporary directory
    filepath = testpath / "path/to/file.sgy"
    filepath.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/invalid_1.0_ascii_header.sgy",
                filepath)

    # Validator should return `None`, and exceptions should not be
    # raised.
    assert validate_files(
        "dataset_identifier",
        testpath,
        tests.conftest.UNIT_TEST_CONFIG_FILE
    ) is None


def test_validate_wrong_mimetype(requests_mock, testpath):
    """Test validating a text file as a TIFF file.

    File validator should detect wrong mimetype, and raise exception.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is image/tiff.
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TIFF_FILE)]
    )

    # Mock Ida. Create a plain text file instead of a TIFF file.
    filepath = testpath / "path/to/file.tiff"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files(
            "dataset_identifier",
            testpath,
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier_tiff']
