"""Tests for `siptools_research.utils.ida` module"""
import os
import pytest
from siptools_research.utils import ida


@pytest.mark.usefixtures('testida')
def test_download_file(testpath):
    """Downloads a file to a temporary directory and checks contents of the
    file.

    :testpath: Temporary directory fixture"""
    new_file_path = os.path.join(testpath, 'new_file')
    ida.download_file('pid:urn:1', new_file_path,
                      pytest.TEST_CONFIG_FILE)

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'


@pytest.mark.usefixtures('testida')
def test_download_big_file_header(testpath):
    """Checks the size of the big file header created is 512 bytes.
    """
    header = ida.download_file_header('valid_tiff.tiff',
                                      pytest.TEST_CONFIG_FILE)

    assert os.path.getsize(header) == 512


@pytest.mark.usefixtures('testida')
def test_download_short_file_header(testpath):
    """Checks the size of the short file header created is correct.
    """
    header = ida.download_file_header('pid:urn:1',
                                      pytest.TEST_CONFIG_FILE)

    # The file should be a text file that says: "foo\n"
    assert os.path.getsize(header) == 4