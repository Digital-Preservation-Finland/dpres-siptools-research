"""Test the :mod:`siptools_research.workflow.get_files` module"""

import os
import pytest
import pymongo
import tests.conftest
from siptools_research.workflow import get_files


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'testida')
def test_getfiles_ida(testpath):
    """Tests for ``GetFiles`` task for IDA files.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :param testpath: Testpath fixture
    :returns: ``None``
    """

    # Create required directories to  workspace
    sipdirectory = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(sipdirectory)
    os.makedirs(os.path.join(testpath, 'logs'))

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset",
                              config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    with open(os.path.join(sipdirectory, 'path/to/file1')) as open_file:
        assert open_file.read() == 'foo\n'

    with open(os.path.join(sipdirectory, 'path/to/file2')) as open_file:
        assert open_file.read() == 'bar\n'


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_getfiles_passipservice(testpath):
    """Tests for ``GetFiles`` task for files that have been directly
    uploaded to passipservice.

    - ``Task.complete()`` is true after ``Task.run()``
    - Symlinks pointing to the files are created to correct paths

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    # Add file identifiers to the mocked mongo
    test_mongo_client = pymongo.MongoClient()
    mongo_files = [
        ("pid:urn:1", "tests/httpretty_data/ida/pid:urn:1"),
        ("pid:urn:2", "tests/httpretty_data/ida/pid:urn:2"),
        ("pid:urn:999", "tests/httpretty_data/ida/pid:urn:999")
    ]
    for identifier, fpath in mongo_files:
        test_mongo_client.upload.files.insert_one(
            {"_id": identifier, "file_path": os.path.abspath(fpath)}
        )


    # Create required directories to  workspace
    sipdirectory = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(sipdirectory)
    os.makedirs(os.path.join(testpath, 'logs'))

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset_local",
                              config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that symlinks to correct files are created into correct path
    assert os.path.isfile(os.path.join(sipdirectory, "path/to/file1"))
    with open(os.path.join(sipdirectory, 'path/to/file1')) as open_file:
        assert open_file.read() == 'foo\n'

    assert os.path.isfile(os.path.join(sipdirectory, "path/to/file2"))
    with open(os.path.join(sipdirectory, 'path/to/file2')) as open_file:
        assert open_file.read() == 'bar\n'


@pytest.mark.usefixtures('testmetax', 'testida')
def test_missing_files_ida(testpath):
    """Test case where a file can not be found from Ida. The first file should
    successfully downloaded, but the second file is not found in Ida. Task
    should fail with Exception.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Init task
    task = get_files.GetFiles(
        workspace=testpath,
        dataset_id="get_files_test_dataset_ida_missing_file",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) == "File /path/to/file4 not found in Ida."

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/path/to/file3'))\
            as open_file:
        assert open_file.read() == 'foo\n'


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_missing_files_local(testpath):
    """Test case where a file can not be found. The first file should
    successfully downloaded, but the second file is not found. Task
    should fail with Exception.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Add file identifiers to the mocked mongo
    test_mongo_client = pymongo.MongoClient()
    mongo_files = [
        ("pid:urn:1", "tests/httpretty_data/ida/pid:urn:1"),
        ("pid:urn:does_not_exist", "file/not/found/on/disk")
    ]
    for identifier, fpath in mongo_files:
        test_mongo_client.upload.files.insert_one(
            {"_id": identifier, "file_path": os.path.abspath(fpath)}
        )


    # Init task
    task = get_files.GetFiles(
        workspace=testpath,
        dataset_id="get_files_test_dataset_local_missing_file",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) == "File /path/to/file4 not found on disk"

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/path/to/file3'))\
            as open_file:
        assert open_file.read() == 'foo\n'
