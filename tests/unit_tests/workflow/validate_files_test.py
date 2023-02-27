"""Unit tests for ValidateFiles task."""
import copy

import pytest

from siptools_research.workflow import validate_files
from siptools_research.exceptions import InvalidFileError
import tests.metax_data.files
import tests.utils


@pytest.mark.usefixtures('testmongoclient')
def test_validatefiles(workspace, requests_mock):
    """Test file validation.

    :param testpath: Temporary directory
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Create a dataset that contains one valid text file which is
    # available in Ida
    textfile = copy.deepcopy(tests.metax_data.files.TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[textfile])
    tests.utils.add_mock_ida_download(requests_mock,
                                      dataset_id='dataset_identifier',
                                      filename='path/to/file',
                                      content=b'foo')

    # Init and run task
    task = validate_files.ValidateFiles(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_validatefiles_invalid(workspace, requests_mock):
    """Test file validation for invalid file.

    Validate dataset that contains invalid file. The task should fail.

    :param testpath: Temporary directory
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Create a dataset that contains one file which has metadata of
    # image file but content of a text file.
    tifffile = copy.deepcopy(tests.metax_data.files.TIFF_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[tifffile])
    tests.utils.add_mock_ida_download(requests_mock,
                                      dataset_id='dataset_identifier',
                                      filename='path/to/file.tiff',
                                      content=b'foo')

    # Init and run task. Running task should raise an exception.
    task = validate_files.ValidateFiles(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    with pytest.raises(InvalidFileError, match='1 files are not well-formed'):
        task.run()
    assert not task.complete()
