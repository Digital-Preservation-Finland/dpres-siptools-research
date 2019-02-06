"""Test the :mod:`siptools_research.workflow.get_files` module"""

import os
import pytest
import pymongo
import tests.conftest
from siptools_research.workflow import get_files


def _init_files_col(mongoclient):
    """Init mocked upload.files collection"""
    mongo_files = [
        ("pid:urn:1", "tests/httpretty_data/ida/pid:urn:1"),
        ("pid:urn:2", "tests/httpretty_data/ida/pid:urn:2"),
        ("pid:urn:999", "tests/httpretty_data/ida/pid:urn:999"),
        ("pid:urn:does_not_exist", "file/not/found/on/disk")
    ]
    for identifier, fpath in mongo_files:
        mongoclient.upload.files.insert_one(
            {"_id": identifier, "file_path": os.path.abspath(fpath)}
        )


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'testida')
def test_getfiles(testpath, file_storage):
    """Tests for ``GetFiles`` task for IDA and local files.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    if file_storage == "local":
        mongoclient = pymongo.MongoClient()
        _init_files_col(mongoclient)

    # Create required directories to  workspace
    sipdirectory = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(sipdirectory)
    os.makedirs(os.path.join(testpath, 'logs'))

    # Init task
    task = get_files.GetFiles(
        workspace=testpath,
        dataset_id="get_files_test_dataset_%s" % file_storage,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    with open(os.path.join(sipdirectory, 'path/to/file1')) as open_file:
        assert open_file.read() == 'foo\n'

    with open(os.path.join(sipdirectory, 'path/to/file2')) as open_file:
        assert open_file.read() == 'bar\n'


@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'testida')
def test_missing_files(testpath, file_storage):
    """Test case where a file can not be found from Ida. The first file should
    successfully downloaded, but the second file is not found in Ida. Task
    should fail with Exception.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    if file_storage == "local":
        mongoclient = pymongo.MongoClient()
        _init_files_col(mongoclient)

    # Init task
    task = get_files.GetFiles(
        workspace=testpath,
        dataset_id="get_files_test_dataset_%s_missing_file" % file_storage,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as excinfo:
        task.run()

    # Check exception message
    if file_storage == "ida":
        assert str(excinfo.value) == "File /path/to/file4 not found in Ida."
    else:
        assert str(excinfo.value) == "File /path/to/file4 not found on disk"

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/path/to/file3'))\
            as open_file:
        assert open_file.read() == 'foo\n'
