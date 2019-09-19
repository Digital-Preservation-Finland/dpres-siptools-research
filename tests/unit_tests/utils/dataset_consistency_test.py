"""dataset_consistency module unit tests"""
import copy
import pytest

from metax_access import Metax
from siptools_research.utils.dataset_consistency import DatasetConsistency
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest


# pylint: disable=invalid-name
def test_verify_file_contained_by_dataset_files():
    """Check that ``DatasetConsistency::is_consistent_for_file()`` succeeds
    when dataset files contains the file

    :returns: ``None``
    """
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    dataset = {
        'identifier': 'dataset_identifier',
        'research_dataset': {'files': [
            {'identifier': 'file_identifier'}
        ], 'directories': []}
    }

    file_metadata = {
        'identifier': 'file_identifier',
        'file_path': "/path/to/file",
        'parent_directory': {'identifier': 'parent_directory_identifier'}
    }
    try:
        dirs = DatasetConsistency(client, dataset)
        dirs.is_consistent_for_file(file_metadata)
    except InvalidMetadataError as exc:
        pytest.fail(
            '_verify_file_contained_by_dataset raised exception: ' + str(exc)
        )


# pylint: disable=invalid-name
def test_verify_file_contained_by_dataset_directories(requests_mock):
    """Check that ``DatasetConsistency::is_consistent_for_file()`` succeeds
    when dataset directories contains the file

    :returns: ``None``
    """
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    dataset = {
        'identifier': 'dataset_identifier',
        'research_dataset': {
            'files': [],
            'directories': [{'identifier': 'root_directory'}]
        }
    }

    file_metadata = {
        'identifier': 'file_identifier',
        'file_path': "/path/to/file",
        'parent_directory': {
            'identifier': 'parent_directory_identifier'
        }
    }
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/parent_directory_identifier',
        json={
            'identifier': 'parent_directory_identifier',
            'parent_directory': {
                'identifier': 'root_directory'
            }
        },
        status_code=200
    )
    try:
        dirs = DatasetConsistency(client, dataset)
        dirs.is_consistent_for_file(file_metadata)
    except InvalidMetadataError as exc:
        pytest.fail(
            '_verify_file_contained_by_dataset raised exception: ' + str(exc)
        )


# pylint: disable=invalid-name
def test_verify_file_contained_by_dataset_missing_from_dataset(requests_mock):
    """Check that ``DatasetConsistency::is_consistent_for_file()`` raises
    exception with descriptive error messages when dataset files nor
    directories do not contain the file.

    :returns: ``None``
    """
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    dataset = {
        'identifier': 'dataset_identifier',
        'research_dataset': {
            'files': [],
            'directories': []
        }
    }

    file_metadata = {
        'identifier': 'file_identifier',
        'file_path': "/path/to/file",
        'parent_directory': {
            'identifier': 'parent_directory_identifier'
        }
    }
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/parent_directory_identifier',
        json={'identifier': 'parent_directory_identifier'},
        status_code=200
    )
    with pytest.raises(InvalidMetadataError) as exc_info:
        dirs = DatasetConsistency(client, dataset)
        dirs.is_consistent_for_file(file_metadata)

    assert str(exc_info.value) == ("File not found from dataset files nor "
                                   "directories: /path/to/file")


# pylint: disable=invalid-name
def test_dataset_directories_caching_works(requests_mock):
    """Checks that caching of dataset directories in``DatasetConsistency``
    works and no extra calls are done to Metax. In this test dataset contains
    only one entry in dataset directories which is the root directory of the
    dataset files:

    /root_dir/second_par_dir/first_par_dir/file1
    /root_dir/second_par_dir/first_par_dir/file1

    :returns: ``None``
    """
    FILE_METADATA = {
        'file_path': "/path/to/file1",
        'parent_directory': {
            'identifier': 'first_par_dir'
        },
        "checksum": {
            "algorithm": "md5",
            "value": "foobar"
        },
        "file_characteristics": {
            "file_format": "text/csv"
        },
        "file_storage": {
            "identifier": "foobar",
            "id": 1
        }
    }
    file_1 = copy.deepcopy(FILE_METADATA)
    file_1['identifier'] = 'file_identifier1'
    file_2 = copy.deepcopy(FILE_METADATA)
    file_2['identifier'] = 'file_identifier2'

    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    dataset = {
        'identifier': 'dataset_identifier',
        'research_dataset': {
            'files': [],
            'directories': [{'identifier': 'root_dir'}]
        }
    }

    first_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/first_par_dir',
        json={
            'identifier': 'first_par_dir',
            'parent_directory': {
                'identifier': 'second_par_dir'
            }
        },
        status_code=200
    )
    second_par_dir_adapter = requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par_dir',
        json={
            'identifier': 'second_par_dir',
            'parent_directory': {
                'identifier': 'root_dir'
            }
        },
        status_code=200
    )
    try:
        dirs = DatasetConsistency(client, dataset)
        dirs.is_consistent_for_file(file_1)
        dirs.is_consistent_for_file(file_2)
    except InvalidMetadataError as exc:
        pytest.fail(
            '_verify_file_contained_by_dataset raised exception: ' + str(exc)
        )
    # verify that dataset directory caching works. Metax is called only once
    # for the parent directories for the two files.
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
