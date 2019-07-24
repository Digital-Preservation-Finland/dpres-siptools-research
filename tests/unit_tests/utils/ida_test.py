"""Tests for :mod:`siptools_research.utils.ida` module"""
import os

import pytest

import tests.conftest
from siptools_research.utils import ida
from siptools_research.utils.ida import IdaError


@pytest.mark.usefixtures('testida')
def test_download_file(testpath):
    """Downloads a file to a temporary directory and checks contents of the
    file.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    new_file_path = os.path.join(testpath, 'new_file')
    ida.download_file('pid:urn:1', new_file_path,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Remove file from ida_files and test that the workspace copy stays intact
    os.remove(os.path.join(testpath, "ida_files", "pid:urn:1"))

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'


@pytest.mark.usefixtures('testida')
def test_download_file_404(testpath):
    """Tries to download non-existing file from IDA.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    new_file_path = os.path.join(testpath, 'new_file')
    with pytest.raises(IdaError):
        ida.download_file('pid:urn:does_not_exist', new_file_path,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
