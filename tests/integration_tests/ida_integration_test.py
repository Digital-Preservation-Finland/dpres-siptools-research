"""Test that download module can download files from Ida test server. The
password for Ida user 'testuser_1' is prompted during the test."""

import os
import getpass
import tests.conftest
from siptools_research.config import Configuration
from siptools_research.utils.download import download_file


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
        getpass.getpass(prompt='Ida password for user \'testuser_1\':')
    )

    # Download a file that is should be available
    download_path = os.path.join(testpath, 'ida_file')
    download_file(
        {
            'file_path': '/file',
            'identifier': 'pid:urn:111',
            'file_storage': {'identifier': 'urn:nbn:fi:att:file-storage-ida'}
        },
        download_path,
        tests.conftest.TEST_CONFIG_FILE
    )

    # Check contents of downloaded file
    with open(download_path) as open_file:
        assert open_file.read() == 'foo\n'
