"""Unit tests for directory_validation module."""
import copy
import pytest

from siptools_research.metax import get_metax_client
import tests.conftest
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.utils.directory_validation import DirectoryValidation

FILE_METADATA = {
    'file_path': "/second_par/first_par/file1",
    'parent_directory': {
        'identifier': 'first_par'
    },
}


# pylint: disable=invalid-name
def test_successful_directory_validation(requests_mock):
    """Directory validation of valid directory tree.

    :returns: ``None``
    """
    # Init metax client
    client = get_metax_client(tests.conftest.UNIT_TEST_CONFIG_FILE)

    first_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={
            'identifier': 'first_par',
            'directory_path': '/second_par/first_par',
            'parent_directory': {
                'identifier': 'second_par'
            }
        },
        status_code=200
    )
    second_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={
            'identifier': 'second_par',
            'directory_path': '/second_par',
            'parent_directory': {
                'identifier': 'root'
            }
        },
        status_code=200
    )
    root_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={
            'identifier': 'root',
            'directory_path': '/'
        },
        status_code=200
    )
    try:
        validator = DirectoryValidation(client)
        validator.is_valid_for_file(FILE_METADATA)
    except InvalidDatasetMetadataError as exc:
        pytest.fail('test_successful_directory_validation fails: ' + str(exc))
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1


# pylint: disable=invalid-name
def test_directory_validation_caching_works(requests_mock):
    """Test directory validation caching.

    Two files are contained by same directory. Metax is called only once for
    each directory in tree and hence the directory validation as well.

    :returns: ``None``
    """
    # Init metax client
    client = get_metax_client(tests.conftest.UNIT_TEST_CONFIG_FILE)

    first_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={
            'identifier': 'first_par',
            'directory_path': '/second_par/first_par',
            'parent_directory': {'identifier': 'second_par'}
        },
        status_code=200
    )
    second_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={
            'identifier': 'second_par',
            'directory_path': '/second_par',
            'parent_directory': {'identifier': 'root'}
        },
        status_code=200
    )
    root_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={
            'identifier': 'root',
            'directory_path': '/'
        },
        status_code=200
    )
    file2_metadata = copy.deepcopy(FILE_METADATA)
    file2_metadata['file_path'] = ["/path/to/file2"]
    try:
        validator = DirectoryValidation(client)
        validator.is_valid_for_file(FILE_METADATA)
        validator.is_valid_for_file(file2_metadata)
    except InvalidDatasetMetadataError as exc:
        pytest.fail(
            'test_successful_directory_validation fails: ' + str(exc)
        )
    # verify that metax is called only once for directories
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1


# pylint: disable=invalid-name
def test_successful_directory_validation_fails(requests_mock):
    """Test validation of invalid directory tree.

    The root directory is missing the `directory_path` attribute

    :returns: ``None``
    """
    # Init metax client
    client = get_metax_client(tests.conftest.UNIT_TEST_CONFIG_FILE)

    first_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={
            'identifier': 'first_par',
            'directory_path': '/second_par/first_par',
            'parent_directory': {'identifier': 'second_par'}
        },
        status_code=200
    )
    second_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={
            'identifier': 'second_par',
            'directory_path': '/second_par',
            'parent_directory': {'identifier': 'root'}
        },
        status_code=200
    )
    root_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={'identifier': 'root'},
        status_code=200
    )
    with pytest.raises(InvalidDatasetMetadataError) as exc_info:
        validator = DirectoryValidation(client)
        validator.is_valid_for_file(FILE_METADATA)

    assert str(exc_info.value).startswith(
        "Validation error in metadata of root: "
        "'directory_path' is a required property"
    )

    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1
