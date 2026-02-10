"""Tests for :mod:`siptools_research.download` module."""
import configparser

import pytest

from siptools_research.download import (
    FileAccessError,
    FileNotAvailableError,
    download_file,
)
from tests.utils import add_mock_ida_file

TEST_DATA = b"foo\n"
TEST_CHECKSUM = \
    "sha256:b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


def _get_file_metadata(identifier, checksum):
    """Return Metax file metadata."""
    return {
        "pathname": "/path/to/file",
        "id": identifier,
        "storage_service": "ida",
        "checksum": checksum,
        "csc_project": "test_project_id"
    }


def test_download_file(config, tmp_path, ida_mount_path):
    """Test downloading a file to a temporary directory.

    :param config: Configuration file
    :param tmp_path: Temporary directory
    :param ida_mount_path: Path to mock IDA mountpoint.
    """
    # Mock ida
    add_mock_ida_file(
        ida_mount_path=ida_mount_path,
        project_id='test_project_id',
        filename="/path/to/file",
        content=TEST_DATA
    )

    new_file_path = tmp_path / 'new_file'
    download_file(
        _get_file_metadata(
            'pid:urn:1',
            checksum=TEST_CHECKSUM
        ),
        "dataset_id",
        new_file_path,
        config
    )

    # The file should be a text file that says: "foo\n"
    assert new_file_path.read_bytes() == TEST_DATA


def test_download_file_invalid_checksum(config, tmp_path, ida_mount_path):
    """Try to download a file from IDA with a non-matching checksum.

    :param config: Configuration file
    :param tmp_path: Temporary path
    :param ida_mount_path: Path to mock IDA mount.
    """
    add_mock_ida_file(
        ida_mount_path=ida_mount_path,
        project_id="test_project_id",
        filename="/path/to/file",
        content=TEST_DATA
    )

    new_file_path = tmp_path / 'new_file'
    with pytest.raises(ValueError) as exc:
        download_file(
            _get_file_metadata(
                'pid:urn:1',
                checksum="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            ),
            "dataset_id",
            new_file_path,
            config
        )

    assert "Computed checksum was different" in str(exc.value)
    assert "Expected fffffffff" in str(exc.value)
    assert "got b5bb9d8014" in str(exc.value)


def test_download_file_404(config, tmp_path):
    """Try to download non-existing file from IDA.

    :param config: Configuration file
    :param tmp_path: Temporary directory
    """
    new_file_path = tmp_path / 'new_file'
    with pytest.raises(FileNotAvailableError):
        download_file(
            _get_file_metadata(
                'pid:urn:does_not_exist', checksum=TEST_CHECKSUM
            ),
            "fake_dataset",
            new_file_path,
            config
        )


def test_download_file_502(config, tmp_path, ida_mount_path):
    """Try to download from Ida when Ida mount is unavailable.

    :param config: Configuration file
    :param tmp_path: Temporary directory fixture
    :param ida_mount_path: Path to IDA mount.
    """
    offline_file = ida_mount_path / 'OFFLINE'
    offline_text = "information from the offline file."
    offline_file.write_text(offline_text)

    new_file_path = tmp_path / 'new_file'
    with pytest.raises(FileAccessError) as exc_info:
        download_file(
            _get_file_metadata(
                'pid:urn:502',
                checksum=TEST_CHECKSUM
            ),
            "fake_dataset",
            new_file_path,
            config
        )
    assert str(exc_info.value) == (f"IDA service offline with message: {offline_text}. "
                                   "Please try again later.")
