"""Tests for :mod:`siptools_research.utils.ida` module"""
import os
import time

import pytest

from siptools_research.utils import ida
from siptools_research.utils.ida import IdaError
import tests.conftest


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


def test_clean_cache(testpath):
    """Test that all the expired files are removed from ida_files.
    """
    ida_files_path = os.path.join(testpath, 'ida_files')

    # Create a fresh file
    fpath_fresh = os.path.join(ida_files_path, "fresh_file")
    with open(fpath_fresh, 'w') as file_:
        file_.write('foo')

    # Create a file older than two weeks
    fpath_expired = os.path.join(ida_files_path, "expired_file")
    with open(fpath_expired, 'w') as file_:
        file_.write('foo')
    expired_access = int(time.time() - (60*60*24*14 + 1))
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than two weeks
    ida.clean_cache(tests.conftest.UNIT_TEST_CONFIG_FILE)

    # ida_files/fresh_file should not be removed
    assert os.path.isfile(fpath_fresh)

    # ida_files/expired_file should be removed
    assert not os.path.isfile(fpath_expired)
