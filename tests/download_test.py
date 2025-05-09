"""Tests for :mod:`siptools_research.download` module."""
import configparser

import pytest

from siptools_research.download import (
    FileAccessError,
    FileNotAvailableError,
    download_file,
)
from tests.utils import add_mock_ida_download

TEST_DATA = b"foo\n"
TEST_CHECKSUM = \
    "sha256:b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


def _get_file_metadata(identifier, checksum):
    """Return Metax file metadata."""
    return {
        "pathname": "/path/to/file",
        "id": identifier,
        "storage_service": "ida",
        "checksum": checksum
    }


@pytest.mark.parametrize("request_verified", [False, True])
def test_download_file(config, tmp_path, requests_mock, request_verified):
    """Test downloading a file to a temporary directory.

    :param config: Configuration file
    :param tmp_path: Temporary directory
    :param requests_mock: HTTP request mocker
    :param request_verified: should HTTP request to Ida be verified?
    """
    # Modify configuration file
    parser = configparser.ConfigParser()
    parser.read(config)
    parser.set(
        "siptools_research",
        "fd_download_service_ssl_verification",
        str(request_verified)
    )
    with open(config, "w") as file:
        parser.write(file)

    # Mock ida
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_id",
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

    # Request should be verified if verification was enabled in
    # configuration file
    assert requests_mock.last_request.verify is request_verified


def test_download_file_invalid_checksum(config, tmp_path, requests_mock):
    """Try to download a file from IDA with a non-matching checksum.

    :param config: Configuration file
    :param tmp_path: Temporary path
    :param requests_mock: HTTP request mocker
    """
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_id",
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


def test_download_file_404(config, tmp_path, requests_mock):
    """Try to download non-existing file from IDA.

    :param config: Configuration file
    :param tmp_path: Temporary directory
    :param requests_mock: HTTP request mocker
    """
    requests_mock.post('https://download.localhost:4431/authorize',
                       status_code=404)

    new_file_path = tmp_path / 'new_file'
    with pytest.raises(FileNotAvailableError):
        download_file(
            _get_file_metadata(
                'pid:urn:does_not_exist', checksum=TEST_CHECKSUM
            ),
            "fake_dataset",
            str(new_file_path),
            config
        )


def test_download_file_502(config, tmp_path, requests_mock):
    """Try to download from Ida when Ida returns 502.

    :param config: Configuration file
    :param tmp_path: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    """
    requests_mock.post('https://download.localhost:4431/authorize',
                       status_code=502)

    new_file_path = tmp_path / 'new_file'
    with pytest.raises(FileAccessError) as exc_info:
        download_file(
            _get_file_metadata(
                'pid:urn:502',
                checksum=TEST_CHECKSUM
            ),
            "fake_dataset",
            str(new_file_path),
            config
        )
    assert str(exc_info.value) == ("Ida service temporarily unavailable. "
                                   "Please, try again later.")
