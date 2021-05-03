"""Test that download module can download files from Ida test server. The
password for Ida user 'testuser_1' is prompted during the test."""

import getpass
import os

import pytest
import tests.conftest
from siptools_research.config import Configuration
from siptools_research.utils.download import (FileNotAvailableError,
                                              download_file)

try:
    from configparser import ConfigParser
except ImportError:  # Python 2
    from ConfigParser import ConfigParser


def get_ida_password():
    """
    Retrieve the Ida password, trying first to read from an existing
    configuration file and then using a password prompt
    """
    try:
        config = ConfigParser()
        config.read("/etc/siptools_research.conf")
        correct_ida_config = (
            config["siptools_research"]["ida_url"]
            == "https://ida.fd-test.csc.fi:4443"
            and config["siptools_research"]["ida_user"] == "testuser_1"
        )

        if correct_ida_config:
            return config["siptools_research"]["ida_password"]
    except KeyError:
        # Config file does not exist
        pass

    return getpass.getpass(prompt="Ida password for user 'testuser_1':")


@pytest.mark.usefixtures("pkg_root")
def test_ida_download(testpath):
    """Download a file from Ida

    :param testpath: temporary directory fixture
    """
    # Read configuration file
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    # Override Ida password in configuration file with real password from
    # the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'ida_password',
        get_ida_password()
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
    """Try downloading a nonexistent file from Ida.
    """
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)

    # Read configuration file
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    # Override Ida password in configuration file with real password from
    # the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'ida_password',
        getpass.getpass(prompt='Ida password for user \'testuser_1\':')
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

    assert exc.value.message == "File '/test_file_nothing.txt' not found in Ida"
