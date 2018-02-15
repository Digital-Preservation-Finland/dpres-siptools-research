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
                      'tests/data/siptools_research.conf')

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'
