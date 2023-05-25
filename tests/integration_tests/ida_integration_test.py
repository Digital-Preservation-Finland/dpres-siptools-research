"""Test that download module can download files from Ida test server.

The IDA token is prompted during the test.
"""

import getpass
import os

from configparser import ConfigParser

import pytest
import tests.conftest
from siptools_research.config import Configuration
from siptools_research.utils.download import (FileNotAvailableError,
                                              download_file)


def get_fd_download_service_token():
    """Retrieve the Fairdata download service token.

    Tries first to read from an existing configuration file and then
    prompts user.
    """
    try:
        config = ConfigParser()
        config.read("/etc/siptools_research.conf")
        if config["siptools_research"]["fd_download_service_token"]:
            return config["siptools_research"]["fd_download_service_token"]
    except KeyError:
        # Config file does not exist
        pass

    return getpass.getpass(prompt="Ida token:")


@pytest.mark.usefixtures("pkg_root")
def test_ida_download(testpath):
    """Download a file from Ida.

    :param testpath: temporary directory fixture
    """
    # Read configuration file
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    # Override Fairdata download service token in configuration file with real
    # token from the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'fd_download_service_token',
        get_fd_download_service_token()
    )

    # Download a file that is should be available
    download_path = str(testpath / 'ida_file')
    download_file(
        file_metadata={
            'file_path': '/test_file.txt',
            'identifier': '6086bf3bcdf25723400588f379535',
            'file_storage': {'identifier': 'pid:urn:storageidentifier1'}
        },
        dataset_id="08cc617d-876f-4b4b-a44e-cc0e2cb6bd11",
        linkpath=download_path,
        config_file=tests.conftest.TEST_CONFIG_FILE
    )

    # Check contents of downloaded file
    with open(download_path) as open_file:
        assert open_file.read() == 'foo'


def test_ida_download_missing(testpath):
    """Try downloading a nonexistent file from Ida."""
    # Read configuration file
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    # Override Fairdata download service token in configuration file with real
    # token from the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'fd_download_service_token',
        get_fd_download_service_token()
    )

    download_path = os.path.join(testpath, 'ida_file')

    with pytest.raises(FileNotAvailableError) as exc:
        download_file(
            file_metadata={
                "file_path": "/test_file_nothing.txt",
                'identifier': '6086bf3bcdf25723400588f379535',
                'file_storage': {'identifier': 'pid:urn:storageidentifier1'}
            },
            dataset_id="08cc617d-876f-4b4b-a44e-cc0e2cb6bd11",
            linkpath=download_path,
            config_file=tests.conftest.TEST_CONFIG_FILE
        )

    assert str(exc.value) == "File '/test_file_nothing.txt' not found in Ida"
