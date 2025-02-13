"""Unit tests for ValidateFiles task."""
import copy

import pytest

import tests.utils
from siptools_research.exceptions import InvalidFileError
from siptools_research.workflow import validate_files


@pytest.mark.usefixtures('testmongoclient')
def test_validatefiles(config, workspace, requests_mock):
    """Test file validation.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    textfile = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[textfile])

    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    # Init and run task
    task = validate_files.ValidateFiles(
        dataset_id=workspace.name,
        config=config,
    )
    assert not task.complete()
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_validatefiles_invalid(config, workspace, requests_mock):
    """Test file validation for invalid file.

    Validate dataset that contains invalid file. The task should fail.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax. Create a dataset that contains one file which has
    # metadata of image file but content of a text file.
    tifffile = copy.deepcopy(tests.metax_data.filesV3.TIFF_FILE)
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[tifffile])

    filepath \
        = workspace / "metadata_generation/dataset_files/path/to/file.tiff"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    # Init and run task. Running task should raise an exception.
    task = validate_files.ValidateFiles(
        dataset_id=workspace.name,
        config=config,
    )
    with pytest.raises(InvalidFileError, match='1 files are not well-formed'):
        task.run()
    assert not task.complete()
