"""Tests for :mod:`siptools_research.file_validator` module."""

import copy
import shutil

import pytest

from siptools_research.exceptions import InvalidFileError
from siptools_research.file_validator import validate_files
from tests.metax_data.files import SEG_Y_FILE, TIFF_FILE, TXT_FILE, CSV_FILE
from tests.utils import add_metax_dataset


def test_validate_files(config, requests_mock, tmp_path):
    """Test validating valid files.

    Validator should not raise any exceptions.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock Metax
    file1 = copy.deepcopy(TXT_FILE)
    file1["id"] = "pid:urn:textfile1"
    file1["pathname"] = "/path/to/file1"
    file2 = copy.deepcopy(TXT_FILE)
    file2["id"] = "pid:urn:textfile2"
    file2["pathname"] = "/path/to/file2"
    add_metax_dataset(requests_mock=requests_mock, files=[file1, file2])

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
    # Mock Metax. Create a dataset that contains one file. The mimetype
    # of the file is text/plain.
    add_metax_dataset(
        requests_mock=requests_mock,
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
    # Mock Metax. Create a dataset that contains one file. The mimetype
    # of the file is application/x.fi-dpres.segy.
    add_metax_dataset(
        requests_mock=requests_mock,
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
    # Mock Metax. Create a dataset that contains one file. The mimetype
    # of the file is image/tiff.
    add_metax_dataset(
        requests_mock=requests_mock,
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


def test_validate_csv_file(config, requests_mock, tmp_path):
    """Test validating a CSV text file with external CSV metadata

    File validator should detect missing end quote and raise exception

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    add_metax_dataset(
        requests_mock=requests_mock,
        files=[copy.deepcopy(CSV_FILE)]
    )

    filepath = tmp_path / "path/to/file.csv"
    filepath.parent.mkdir(parents=True)
    filepath.write_text(
        "'a';'b';'c'\n"
        "'1';'2';'3'\n"
        "'3a';'4b';'5c"  # Missing end quote
    )

    with pytest.raises(InvalidFileError) as exception_info:
        validate_files("dataset_identifier", tmp_path, config)

    assert str(exception_info.value) == "1 files are not well-formed"
    assert exception_info.value.files == ['pid:urn:identifier_csv']
