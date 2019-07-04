"""Test the :mod:`siptools_research.workflow.get_files` module"""
import os

import pytest
import pymongo

import tests.conftest
import siptools_research.config
from siptools_research.utils.ida import IdaError
from siptools_research.workflow.get_files import UploadApiError
from siptools_research.workflow import get_files
from siptools_research.workflowtask import InvalidMetadataError


def _init_files_col(mongoclient):
    """Init mocked upload.files collection"""
    mongo_files = [
        ("pid:urn:get_files_1_local", "tests/httpretty_data/ida/pid:urn:1"),
        ("pid:urn:get_files_2_local", "tests/httpretty_data/ida/pid:urn:2"),
        ("pid:urn:does_not_exist_local", "file/not/found/on/disk")
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

    error = IdaError if file_storage == "ida" else UploadApiError

    # Run task.
    with pytest.raises(error) as excinfo:
        task.run()

    # Check exception message
    if file_storage == "ida":
        assert str(excinfo.value) == "File /path/to/file4 not found in Ida."
    else:
        assert str(excinfo.value) == "File /path/to/file4 not found on disk"

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/path/to/file1'))\
            as open_file:
        assert open_file.read() == 'foo\n'


@pytest.mark.usefixtures('testida')
@pytest.mark.parametrize('path', ["../../file1",
                                  "/../../file1",
                                  "//../../file1"])
def test_forbidden_relative_path(testpath, path):
    """Test that files can not be saved outside the workspace by using relative
    file paths in Metax. The tested path would be downloaded to
    `<workspace_root>/<workspace>/<sip_creation_path>/../../file1` which equals
    to `<workspace_root>/file1`, if the path was not validated.

    :param testpath: Temporary workspace path fixture
    :param path: sample file path
    :returns: ``None``
    """
    # Create the workspace and required directories
    workspace = os.path.join(testpath, 'workspace')
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Init task
    task = get_files.GetFiles(
        workspace=workspace,
        dataset_id="foo",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    files = [
        {
            "file_path": path,
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            }
        }
    ]
    pas2ida = {"pid:urn:1": "pid:urn:1"}

    # File download should fail
    with pytest.raises(InvalidMetadataError) as exception_info:
        # pylint: disable=protected-access
        task._download_files(
            files, pas2ida, siptools_research.config.Configuration(task.config)
        )
    assert exception_info.value.message \
        == 'The file path of file pid:urn:1 is invalid: %s' % path

    # Check that file is not saved in workspace root i.e. workspace root
    # contains only the workspace directory
    assert set(os.listdir(testpath)) == {'workspace', 'tmp', 'ida_files'}


@pytest.mark.usefixtures('testida')
@pytest.mark.parametrize('path', ["foo/../file1",
                                  "/foo/../file1",
                                  "./file1",
                                  "././file1",
                                  "/./file1"])
def test_allowed_relative_paths(testpath, path):
    """Test that file is downloaded to correct location in some special cases.

    :param testpath: Temporary workspace path fixture
    :param path: sample file path
    :returns: ``None``
    """
    # Create the workspace and required directories
    workspace = os.path.join(testpath, 'workspace')
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Init task
    task = get_files.GetFiles(
        workspace=workspace,
        dataset_id="foo",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    files = [
        {
            "file_path": path,
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            }
        }
    ]
    pas2ida = {"pid:urn:1": "pid:urn:1"}

    # Download file and check that is found in expected location
    # pylint: disable=protected-access
    task._download_files(
        files, pas2ida, siptools_research.config.Configuration(task.config)
    )
    assert os.listdir(sipdirectory) == ['file1']
