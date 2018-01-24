"""Test that ida module can download files from Ida test server. The password
for Ida user 'testuser_1' is prompted during the test."""

import os
import getpass
from siptools_research.config import Configuration
from siptools_research.utils.ida import download_file


def test_ida_download(testpath):
    """Add test dataset metadata and associated file metadata to Metax. Run
    partial workflow by calling CreateMets task with luigi.
    """
    # Read configuration file
    conf = Configuration('tests/data/siptools_research.conf')
    # Override Ida password in configuration file with real password from
    # the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'ida_password',
        getpass.getpass(prompt='Ida password for user \'testuser_1\':')
    )

    # Download a file that is should be available
    download_path = os.path.join(testpath, 'ida_file')
    download_file('pid:urn:1', download_path,
                  'tests/data/siptools_research.conf')

    # Check contents of downloaded file
    with open(download_path) as open_file:
        assert open_file.read() == 'foo\n'
