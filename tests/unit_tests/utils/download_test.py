"""Tests for :mod:`siptools_research.utils.download` module."""
import os
import time

import pytest
from siptools_research.utils.download import (FileAccessError,
                                              FileNotAvailableError,
                                              clean_file_cache, download_file)

from tests.conftest import UNIT_TEST_CONFIG_FILE, UNIT_TEST_SSL_CONFIG_FILE
from tests.utils import add_mock_ida_download


def _get_file_metadata(identifier):
    """Return Metax file metadata."""
    return {
        "file_path": "/path/to/file",
        "identifier": identifier,
        "file_storage": {"identifier": "urn:nbn:fi:att:file-storage-ida"}
    }


@pytest.mark.usefixtures("mock_ida_download")
@pytest.mark.parametrize(('config_file', 'request_verified'),
                         [
                             (UNIT_TEST_CONFIG_FILE, False),
                             (UNIT_TEST_SSL_CONFIG_FILE, True)
                         ])
def test_download_file(pkg_root, requests_mock, config_file, request_verified):
    """Test downloading a file to a temporary directory.

    :param pkg_root: Temporary packaging root directory fixture
    :param requests_mock: HTTP request mocker
    :param config_file: used configuration file
    :param request_verified: should HTTP request to Ida be verified?
    :returns: ``None``
    """
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_id",
        filename="/path/to/file",
        content=b"foo\n"
    )
    requests_mock.get("https://ida.dl.test/download",
                      content=b"foo\n")

    new_file_path = pkg_root / 'new_file'
    download_file(
        _get_file_metadata('pid:urn:1'),
        "dataset_id",
        str(new_file_path),
        config_file
    )

    # Remove file from file_cache and test that the workspace copy stays
    # intact
    (pkg_root / "file_cache" / "pid:urn:1").unlink()

    # The file should be a text file that says: "foo\n"
    assert new_file_path.read_text() == "foo\n"

    assert requests_mock.last_request.verify is request_verified


def test_download_file_404(pkg_root, requests_mock):
    """Try to download non-existing file from IDA.

    :param pkg_root: Temporary packaging root directory fixture
    :returns: ``None``
    """
    requests_mock.post('https://ida.dl-authorize.test/authorize',
                      status_code=404)

    new_file_path = pkg_root / 'new_file'
    with pytest.raises(FileNotAvailableError):
        download_file(
            _get_file_metadata('pid:urn:does_not_exist'),
            "fake_dataset",
            str(new_file_path),
            UNIT_TEST_CONFIG_FILE
        )


def test_download_file_502(pkg_root, requests_mock):
    """Try to download from Ida when Ida returns 502.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.post('https://ida.dl-authorize.test/authorize',
                      status_code=502)

    new_file_path = pkg_root / 'new_file'
    with pytest.raises(FileAccessError) as exc_info:
        download_file(
            _get_file_metadata('pid:urn:502'),
            "fake_dataset",
            str(new_file_path),
            UNIT_TEST_CONFIG_FILE
        )
    assert str(exc_info.value) == ("Ida service temporarily unavailable. "
                                   "Please, try again later.")


def test_clean_file_cache(pkg_root):
    """Test that all the expired files are removed from file_cache."""
    cache_path = pkg_root / 'file_cache'

    # Create a fresh file
    fpath_fresh = cache_path / "fresh_file"
    fpath_fresh.write_text("foo")

    # Create a file older than two weeks
    fpath_expired = cache_path / "expired_file"
    fpath_expired.write_text("foo")
    expired_access = int(time.time() - (60*60*24*14 + 1))
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than two weeks
    clean_file_cache(UNIT_TEST_CONFIG_FILE)

    # file_cache/fresh_file should not be removed
    assert fpath_fresh.is_file()

    # file_cache/expired_file should be removed
    assert not fpath_expired.is_file()
