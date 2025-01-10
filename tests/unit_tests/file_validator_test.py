"""Tests for :mod:`siptools_research.file_validator` module."""

import copy
import shutil

import pytest

from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import SEG_Y_FILE, TIFF_FILE, TXT_FILE
from tests.utils import add_metax_v2_dataset


def test_validate_files(config, requests_mock, tmp_path):
    """Test validating valid files.

    Validator should not raise any exceptions.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock metax
    file1 = copy.deepcopy(TXT_FILE)
    file1['identifier'] = 'pid:urn:textfile1'
    file1['file_path'] = '/path/to/file1'
    file2 = copy.deepcopy(TXT_FILE)
    file2['identifier'] = 'pid:urn:textfile2'
    file2['file_path'] = '/path/to/file2'
    add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[file1, file2]
    )

    # Create valid files in temporary directory
    path1 = tmp_path / "path/to/file1"
    path1.parent.mkdir(parents=True)
    path1.write_text("foo")
    path2 = tmp_path / "path/to/file2"
    path2.write_text("bar")

    # Validator should return `None`, and exceptions should not be
    # raised.
    assert validate_files("dataset_identifier", tmp_path, config) is None


def test_validate_invalid_files(config, requests_mock, tmp_path):
    """Test validating a not well-formed file.

    Try to validate an empty text file. File validator should detect the
    file as not well-formed and raise exception.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is text/plain.
    add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TXT_FILE)]
    )

    # Create a empty file to temporary directory
    filepath = tmp_path / "path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text("")

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files( "dataset_identifier", tmp_path, config)

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier']


def test_validate_bitlevel_file(config, requests_mock, tmp_path):
    """Test validating a file only accepted for bit-level preservation.

    File validator should ignore the validation result in this case.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is application/x.fi-dpres.segy.
    add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(SEG_Y_FILE)]
    )

    # Copy a SEG-Y file to temporary directory
    filepath = tmp_path / "path/to/file.sgy"
    filepath.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/invalid_1.0_ascii_header.sgy",
                filepath)

    # Validator should return `None`, and exceptions should not be
    # raised.
    assert validate_files("dataset_identifier", tmp_path, config) is None


def test_validate_wrong_mimetype(config, requests_mock, tmp_path):
    """Test validating a text file as a TIFF file.

    File validator should detect wrong mimetype, and raise exception.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock metax. Create a dataset that contains one file. The mimetype
    # of the file is image/tiff.
    add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=copy.deepcopy(BASE_DATASET),
        files=[copy.deepcopy(TIFF_FILE)]
    )

    # Mock Ida. Create a plain text file instead of a TIFF file.
    filepath = tmp_path / "path/to/file.tiff"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files("dataset_identifier", tmp_path, config)

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier_tiff']
