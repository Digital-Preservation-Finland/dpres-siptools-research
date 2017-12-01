"""Tests for `siptools_research.utils.ida` module"""
import os
from siptools_research.utils import ida

def test_download_file(testpath, testida):
    """Downloads a file to a temporary directory and checks contents of the
    file"""
    new_file_path = os.path.join(testpath, 'new_file')
    ida.download_file('pid:urn:1', new_file_path,
                      'tests/data/siptools_research.conf')

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'
