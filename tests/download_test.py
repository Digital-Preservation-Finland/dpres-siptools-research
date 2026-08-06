"""Tests for :mod:`siptools_research.download` module."""
import shutil

import pytest

from siptools_research.download import (FileAccessError, FileNotAvailableError,
                                        IDAFileAccessError,
                                        download_file)
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
    with pytest.raises(ValueError, match="checksum was different") as exc:
        download_file(
            _get_file_metadata(
                'pid:urn:1',
                checksum="sha256:fffffffffffffffffffffffffffffffffffffffffffff"
            ),
            new_file_path,
            config
        )

    assert "Computed checksum was different" in str(exc.value)
    assert "Expected fffffffff" in str(exc.value)
    assert "got b5bb9d8014" in str(exc.value)


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
            new_file_path,
            config
        )
    assert str(exc_info.value) \
        == (f"IDA service offline with message: {offline_text}. "
            "Please try again later.")


@pytest.mark.parametrize(
    "hide_project_dir,expected",
    [
        (True, "Project directory not available"),
        (False, "Project directory exists, but file is missing")
    ]
)
def test_download_ida_missing_file(
        config, tmp_path, ida_mount_path, hide_project_dir, expected):
    """
    Try to download file from Ida while either the project directory
    or only the file is missing. Both cases should result in different
    error messages.
    """
    add_mock_ida_file(
        ida_mount_path=ida_mount_path,
        project_id='test_project_id',
        filename="/path/to/file",
        content=TEST_DATA
    )


    if hide_project_dir:
        shutil.rmtree(ida_mount_path / "test_project_id")
    else:
        (ida_mount_path \
            / "test_project_id/test_project_id/path/to/file").unlink()

    with pytest.raises(IDAFileAccessError) as exc:
        download_file(
            _get_file_metadata(
                'pid:urn:1',
                checksum=TEST_CHECKSUM
            ),
            "/dev/null",
            config
        )

    assert expected in str(exc.value)
