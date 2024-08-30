"""Tests for :mod:`siptools_research.utils.download` module."""

import pytest
from siptools_research.utils.download import (FileAccessError,
                                              FileNotAvailableError,
                                              download_file)

from tests.conftest import UNIT_TEST_CONFIG_FILE, UNIT_TEST_SSL_CONFIG_FILE
from tests.utils import add_mock_ida_download


def _get_file_metadata(identifier):
    """Return Metax file metadata."""
    return {
        "pathname": "/path/to/file",
        "id": identifier,
        "storage_identifier": "urn:nbn:fi:att:file-storage-ida"
    }


@pytest.mark.parametrize(('config_file', 'request_verified'),
                         [
                             (UNIT_TEST_CONFIG_FILE, False),
                             (UNIT_TEST_SSL_CONFIG_FILE, True)
                         ])
def test_download_file(testpath, requests_mock, config_file, request_verified):
    """Test downloading a file to a temporary directory.

    :param testpath: Temporary directory
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
    requests_mock.get("https://download.dl.test/download",
                      content=b"foo\n")

    new_file_path = testpath / 'new_file'
    download_file(
        _get_file_metadata('pid:urn:1'),
        "dataset_id",
        new_file_path,
        config_file
    )

    # The file should be a text file that says: "foo\n"
    assert new_file_path.read_text() == "foo\n"

    assert requests_mock.last_request.verify is request_verified


def test_download_file_404(testpath, requests_mock):
    """Try to download non-existing file from IDA.

    :param testpath: Temporary directory
    :returns: ``None``
    """
    requests_mock.post('https://download.dl-authorize.test/authorize',
                       status_code=404)

    new_file_path = testpath / 'new_file'
    with pytest.raises(FileNotAvailableError):
        download_file(
            _get_file_metadata('pid:urn:does_not_exist'),
            "fake_dataset",
            str(new_file_path),
            UNIT_TEST_CONFIG_FILE
        )


def test_download_file_502(testpath, requests_mock):
    """Try to download from Ida when Ida returns 502.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.post('https://download.dl-authorize.test/authorize',
                       status_code=502)

    new_file_path = testpath / 'new_file'
    with pytest.raises(FileAccessError) as exc_info:
        download_file(
            _get_file_metadata('pid:urn:502'),
            "fake_dataset",
            str(new_file_path),
            UNIT_TEST_CONFIG_FILE
        )
    assert str(exc_info.value) == ("Ida service temporarily unavailable. "
                                   "Please, try again later.")
