"""Test the :mod:`siptools_research.workflow.get_files` module."""
import os

import pymongo
import pytest
import tests.conftest
from siptools_research.exceptions import InvalidFileMetadataError
from siptools_research.utils.download import FileNotAvailableError
from siptools_research.workflow import get_files
from tests.utils import add_metax_dataset, add_mock_ida_download


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_getfiles(workspace, requests_mock):
    """Tests for ``GetFiles`` task for IDA and local files.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="get_files_test_dataset",
        filename="/path/to/file1",
        content=b"foo\n"
    )
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="get_files_test_dataset",
        filename="/path/to/file2",
        content=b"bar\n"
    )

    # Init task
    task = get_files.GetFiles(
        workspace=str(workspace),
        dataset_id="get_files_test_dataset",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    dataset_files_dir = workspace / "dataset_files" / "path" / "to"
    assert (dataset_files_dir / "file1").read_text() == "foo\n"
    assert (dataset_files_dir / "file2").read_text() == "bar\n"


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_missing_ida_files(workspace, requests_mock):
    """Test task when a file can not be found from Ida.

    The first file should successfully downloaded, but the second file
    is not found. Task should fail with Exception.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="get_files_test_dataset_ida_missing_file",
        filename="/path/to/file1",
        content=b"foo\n"
    )

    requests_mock.post(
        'https://ida.dl-authorize.test/authorize',
        status_code=404,
        additional_matcher=lambda req: req.json()["file"] == "/path/to/file4"
    )

    # Init task
    task = get_files.GetFiles(
        workspace=str(workspace),
        dataset_id="get_files_test_dataset_ida_missing_file",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    with pytest.raises(FileNotAvailableError) as excinfo:
        task.run()

    assert str(excinfo.value) == "File '/path/to/file4' not found in Ida"

    # Task should not be completed
    assert not task.complete()

    # Nothing should be written to workspace/dataset_files
    assert not (workspace / 'dataset_files').exists()


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_missing_local_files(testpath, workspace, requests_mock):
    """Test task when a local file is not available.

    The first file should successfully downloaded, but the second file
    is not found. Task should fail with Exception.

    :param testpath: Temporary directory fixture
    :param workspace: Temporary workspace directory fixture
    :param requests_mock: requests_mock mocker
    :returns: ``None``
    """
    # Init mocked upload.files collection
    mongoclient = pymongo.MongoClient()
    mongo_files = [
        ("pid:urn:get_files_1_local", str(testpath / "file1")),
        ("pid:urn:does_not_exist_local", str(testpath / "file2"))
    ]
    for identifier, fpath in mongo_files:
        mongoclient.upload.files.insert_one(
            {"_id": identifier, "file_path": str(fpath)}
        )
        add_mock_ida_download(
            requests_mock=requests_mock,
            dataset_id="get_files_test_dataset_local_missing_file",
            filename=fpath,
            content=b"foo\n"
        )

    # Create only the first file in test directory
    (testpath / "file1").write_text("foo\n")

    # Init task
    task = get_files.GetFiles(
        workspace=str(workspace),
        dataset_id="get_files_test_dataset_local_missing_file",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    with pytest.raises(FileNotAvailableError) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) \
        == "File '/path/to/file4' not found in pre-ingest file storage"

    # Task should not be completed
    assert not task.complete()

    # Nothing should be written to workspace/dataset_files
    assert not (workspace / 'dataset_files').exists()


@pytest.mark.parametrize('path', ["../../file1",
                                  "/../../file1",
                                  "//../../file1"])
def test_forbidden_relative_path(pkg_root, workspace, requests_mock, path):
    """Test that files can not be saved outside the workspace.

    Saving files outside the workspace by using relative file paths in
    Metax should not be possible. The tested path would be downloaded to
    `<packaging_root>/workspaces/<workspace>/dataset_files/../../file1`
    which equals to `<packaging_root>/workspaces/file1`, if the path was
    not validated.

    :param pkg_root: Temporary packaging root fixture
    :param workspace: Temporary workspace path fixture
    :param requests_mock: Request mocker
    :param path: sample file path
    :returns: ``None``
    """
    # Mock metax
    files = [
        {
            "file_path": path,
            "parent_directory": {'identifier': 'foo'},
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            }
        }
    ]
    add_metax_dataset(requests_mock, files=files)

    # Init task
    task = get_files.GetFiles(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # File download should fail
    with pytest.raises(InvalidFileMetadataError) as exception_info:
        task.run()
    assert str(exception_info.value) == \
        f'The file path of file pid:urn:1 is invalid: {path}'

    # Check that file is not saved in workspace root i.e. workspace root
    # contains only the workspace directory
    files = set(path.name for path in pkg_root.iterdir())
    assert files == {'workspaces', 'tmp', 'file_cache'}


@pytest.mark.parametrize('path', ["foo/../file1",
                                  "/foo/../file1",
                                  "./file1",
                                  "././file1",
                                  "/./file1"])
def test_allowed_relative_paths(workspace, requests_mock, path):
    """Test that file is downloaded to correct location.

    :param workspace: Temporary workspace path fixture
    :param requests_mock: Request mocker
    :param path: sample file path
    :returns: ``None``
    """
    # Mock Ida and Metax
    requests_mock.get('https://ida.test/files/pid:urn:1/download')
    files = [
        {
            "file_path": path,
            "parent_directory": {'identifier': 'foo'},
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            }
        }
    ]
    add_metax_dataset(requests_mock, files=files)
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename=path,
        content=b"foo\n"
    )

    # Init task
    task = get_files.GetFiles(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Download file and check that is found in expected location
    task.run()
    files = [path.name for path in (workspace / "dataset_files").iterdir()]
    assert files == ['file1']
