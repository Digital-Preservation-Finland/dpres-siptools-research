"""Tests for :mod:`siptools_research.utils.ida` module"""
import os
import pytest
import tests.conftest
from siptools_research.utils import ida
from requests.exceptions import HTTPError


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

    # The file should be a text file that says: "foo\n"
    with open(new_file_path, 'r') as new_file:
        assert new_file.read() == 'foo\n'


@pytest.mark.usefixtures('testida')
def test_download_file_404(testpath):
    """Tries to download non-exiting file from IDA.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    new_file_path = os.path.join(testpath, 'new_file')
    with pytest.raises(HTTPError):
        ida.download_file('pid:urn:does_not_exist', new_file_path,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


@pytest.mark.usefixtures('testida')
def test_download_big_file_header(testpath):
    """Checks the size of the big file header created is 512 bytes.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    new_file_path = os.path.join(testpath, 'new_file')
    ida.download_file_header('pid:urn:1024kB_file', new_file_path,
                             tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert os.path.getsize(new_file_path) == 512


@pytest.mark.usefixtures('testida')
def test_download_short_file_header(testpath):
    """Checks the size of the short file header created is correct.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    new_file_path = os.path.join(testpath, 'new_file')
    ida.download_file_header('pid:urn:1', new_file_path,
                             tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert os.path.getsize(new_file_path) == 4
