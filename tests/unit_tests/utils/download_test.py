"""Tests for :mod:`siptools_research.utils.download` module."""
import os
import time

import pytest

from siptools_research.utils.download import (
    download_file, clean_file_cache,
    FileNotAvailableError, FileAccessError
)
from tests.conftest import UNIT_TEST_CONFIG_FILE


def _get_file_metadata(identifier):
    """Return Metax file metadata."""
    return {
        "file_path": "/path/to/file",
        "identifier": identifier,
        "file_storage": {"identifier": "urn:nbn:fi:att:file-storage-ida"}
    }


def test_download_file(testpath, requests_mock):
    """Test downloading a file to a temporary directory.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.get("https://ida.test/files/pid:urn:1/download",
                      content=b"foo\n")

    new_file_path = os.path.join(testpath, 'new_file')
    download_file(
        _get_file_metadata('pid:urn:1'),
        new_file_path,
        UNIT_TEST_CONFIG_FILE
    )

    # Remove file from file_cache and test that the workspace copy stays intact
    os.remove(os.path.join(testpath, "file_cache", "pid:urn:1"))

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'


def test_download_file_404(testpath, requests_mock):
    """Try to download non-existing file from IDA.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.get('https://ida.test/files/pid:urn:does_not_exist/download',
                      status_code=404)

    new_file_path = os.path.join(testpath, 'new_file')
    with pytest.raises(FileNotAvailableError):
        download_file(
            _get_file_metadata('pid:urn:does_not_exist'),
            new_file_path,
            UNIT_TEST_CONFIG_FILE
        )


def test_download_file_502(testpath, requests_mock):
    """Try to download from Ida when Ida returns 502.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.get('https://ida.test/files/pid:urn:502/download',
                      status_code=502)

    new_file_path = os.path.join(testpath, 'new_file')
    with pytest.raises(FileAccessError) as exc_info:
        download_file(
            _get_file_metadata('pid:urn:502'),
            new_file_path,
            UNIT_TEST_CONFIG_FILE
        )
    assert str(exc_info.value) == ("Ida service temporarily unavailable. "
                                   "Please, try again later.")


def test_clean_file_cache(testpath):
    """Test that all the expired files are removed from file_cache."""
    cache_path = os.path.join(testpath, 'file_cache')

    # Create a fresh file
    fpath_fresh = os.path.join(cache_path, "fresh_file")
    with open(fpath_fresh, 'w') as file_:
        file_.write('foo')

    # Create a file older than two weeks
    fpath_expired = os.path.join(cache_path, "expired_file")
    with open(fpath_expired, 'w') as file_:
        file_.write('foo')
    expired_access = int(time.time() - (60*60*24*14 + 1))
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than two weeks
    clean_file_cache(UNIT_TEST_CONFIG_FILE)

    # file_cache/fresh_file should not be removed
    assert os.path.isfile(fpath_fresh)

    # file_cache/expired_file should be removed
    assert not os.path.isfile(fpath_expired)
