"""Test the :mod:`siptools_research.workflow.get_files` module."""
import copy

import pytest
from upload_rest_api.models.file_entry import FileEntry

from siptools_research.exceptions import (
    InvalidFileMetadataError,
    MissingFileError,
)
from siptools_research.workflow import get_files
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import PAS_STORAGE_SERVICE, TXT_FILE
from tests.utils import add_metax_v2_dataset, add_mock_ida_download


@pytest.mark.usefixtures('testmongoclient')
def test_getfiles(config, workspace, requests_mock):
    """Tests for ``GetFiles`` task for IDA and local files.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :param config: Configuration file
    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax. Create a dataset that contains two text files.
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = 'pid:urn:1'
    files[0]['file_path'] = '/path/to/file1'
    files[0]['checksum'] = {
        'algorithm': 'MD5',
        'value': 'd3b07384d113edec49eaa6238ad5ff00'
    }
    files[1]['identifier'] = 'pid:urn:2'
    files[1]['file_path'] = '/path/to/file2'
    files[1]['checksum'] = {
        'algorithm': 'MD5',
        'value': 'c157a79031e1c40f85931829bc5fc552'
    }
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=files)

    # Mock Ida. Add the two text files to Ida.
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="/path/to/file1",
        content=b"foo\n"
    )
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="/path/to/file2",
        content=b"bar\n"
    )

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    dataset_files_dir \
        = workspace / "metadata_generation" / "dataset_files" / "path" / "to"
    assert (dataset_files_dir / "file1").read_text() == "foo\n"
    assert (dataset_files_dir / "file2").read_text() == "bar\n"


@pytest.mark.usefixtures('testmongoclient')
def test_file_cache(config, workspace, requests_mock):
    """Test that files are downloaded only once.

    If files have already been been downloaded to file cache, they
    should not be downloaded from Ida again.

    :param config: Configuration file
    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax. Create a dataset that contains a text file.
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=[TXT_FILE])

    # Mock Ida
    mock_download = add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="path/to/file",
        content=b"Content of Ida file\n"
    )

    # Create file to file cache directory
    cached_file = workspace / f'file_cache/{TXT_FILE["identifier"]}'
    cached_file.parent.mkdir()
    cached_file.write_text('Content of cached file')

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that the file from file cache was copied to "dataset_files"
    path = workspace / "metadata_generation/dataset_files/path/to/file"
    assert path.read_text() == "Content of cached file"

    # Requests to Ida should not be made
    assert not mock_download.called


@pytest.mark.usefixtures('testmongoclient')
def test_missing_ida_files(config, workspace, requests_mock):
    """Test task when a file can not be found from Ida.

    The first file should successfully downloaded, but the second file
    is not found. Task should fail with Exception.

    :param config: Configuration file
    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax. Create a dataset that contains two text files.
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['file_path'] = '/path/to/file1'
    files[1]['identifier'] = 'pid:urn:not-found-in-ida'
    files[1]['file_path'] = '/path/to/file2'
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=files)

    # Mock Ida. First file can be downloaded, but requesting the second
    # file will cause 404 "Not found" error.
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="/path/to/file1",
        content=b"foo\n"
    )
    requests_mock.post(
        'https://download.localhost:4431/authorize',
        status_code=404,
        additional_matcher=lambda req: req.json()["file"] == "/path/to/file2"
    )

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task.
    with pytest.raises(MissingFileError) as excinfo:
        task.run()

    assert str(excinfo.value) == "1 files are missing"

    # Task should not be completed
    assert not task.complete()

    # Nothing should be written to dataset_files directory
    assert not (workspace / 'metadata_generation' / 'dataset_files').exists()

    # But the first file should be cached
    assert [path.name for path in (workspace / 'file_cache').iterdir()] \
        == ['pid:urn:identifier']


@pytest.mark.usefixtures('testmongoclient')
def test_missing_local_files(config, workspace, requests_mock,
                             upload_projects_path):
    """Test task when a local file is not available.

    The first file should successfully downloaded, but the second file
    is not found. Task should fail with Exception.

    :param config: Configuration file
    :param workspace: Temporary workspace directory fixture
    :param requests_mock: requests_mock mocker
    """
    # Mock metax
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = 'pid:urn:get_files_1_local'
    files[0]['file_storage']['identifier'] = PAS_STORAGE_SERVICE
    files[0]['file_path'] = '/path/to/file1'
    files[1]['identifier'] = 'pid:urn:does_not_exist_local'
    files[1]['file_storage']['identifier'] = PAS_STORAGE_SERVICE
    files[1]['file_path'] = '/path/to/file4'
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=files)

    # Init mocked upload.files collection
    mongo_files = [
        ("pid:urn:get_files_1_local",
         str(upload_projects_path / "path/to/file1")),
        ("pid:urn:does_not_exist_local",
         str(upload_projects_path / "path/to/file4"))
    ]
    for identifier, fpath in mongo_files:
        FileEntry(
            path=str(fpath),
            identifier=identifier,
            checksum="2eeecd72c567401e6988624b179d0b14"
        ).save()

    # Create only the first file in test directory
    (upload_projects_path / "path/to/file1").parent.mkdir(parents=True)
    (upload_projects_path / "path/to/file1").write_text("foo\n")

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task.
    with pytest.raises(MissingFileError) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) == "1 files are missing"

    # Task should not be completed
    assert not task.complete()

    # Nothing should be written to dataset_files directory
    assert not (workspace / 'metadata_generation' / 'dataset_files').exists()

    # But the first file should be cached
    assert [path.name for path in (workspace / 'file_cache').iterdir()] \
        == ['pid:urn:get_files_1_local']


@pytest.mark.parametrize('path', ["../../file1",
                                  "/../../file1",
                                  "//../../file1"])
def test_forbidden_relative_path(config, workspace, requests_mock, path):
    """Test that files can not be saved outside the workspace.

    Saving files outside the workspace by using relative file paths in
    Metax should not be possible. The tested path would be downloaded to
    `<packaging_root>/workspaces/<workspace>/dataset_files/../../file1`
    which equals to `<packaging_root>/workspaces/file1`, if the path was
    not validated.

    :param config: Configuration file
    :param workspace: Temporary workspace fixture
    :param requests_mock: Request mocker
    :param path: sample file path
    """
    # Mock metax
    files = [
        {
            "file_path": path,
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            }
        }
    ]
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=files)

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)

    # File download should fail
    with pytest.raises(InvalidFileMetadataError) as exception_info:
        task.run()
    assert str(exception_info.value) == \
        f'The file path of file pid:urn:1 is invalid: {path}'

    # Check that file is not saved in workspace root i.e. workspace root
    # contains only the workspace directory
    files = {path.name for path in workspace.parent.iterdir()}
    assert files == {workspace.name}


@pytest.mark.parametrize('path', ["foo/../file1",
                                  "/foo/../file1",
                                  "./file1",
                                  "././file1",
                                  "/./file1"])
def test_allowed_relative_paths(config, workspace, requests_mock, path):
    """Test that file is downloaded to correct location.

    :param config: Configuration file
    :param workspace: Temporary workspace path fixture
    :param requests_mock: Request mocker
    :param path: sample file path
    """
    # Mock Ida and Metax
    requests_mock.get('https://ida.test/files/pid:urn:1/download')
    files = [
        {
            "file_path": path,
            "identifier": "pid:urn:1",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            },
            "checksum": {
                "algorithm": "MD5",
                "value": "d3b07384d113edec49eaa6238ad5ff00"
            }
        }
    ]
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(requests_mock, dataset=dataset, files=files)
    add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename=path,
        content=b"foo\n"
    )

    # Init task
    task = get_files.GetFiles(dataset_id=workspace.name, config=config)

    # Download file and check that is found in expected location
    task.run()
    files = [path.name for path
             in (workspace / "metadata_generation/dataset_files").iterdir()]
    assert files == ['file1']
