import copy
import pytest
import requests_mock

from metax_access import Metax
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest
from siptools_research.utils.directory_validation import DirectoryValidation

FILE_METADATA = {
    'file_path': "/second_par/first_par/file1",
    'parent_directory': {
        'identifier': 'first_par'
    },
}


@requests_mock.Mocker()
# pylint: disable=invalid-name
def test_successful_directory_validation(mocker):
    """Directory validation of /second_par/first_par/file1
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
    first_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={'identifier': 'first_par',
              'directory_path': '/second_par/first_par',
              'parent_directory': {
                  'identifier': 'second_par'
                  }
              },
        status_code=200
    )
    second_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={'identifier': 'second_par',
              'directory_path': '/second_par',
              'parent_directory': {
                  'identifier': 'root'
                  }
              },
        status_code=200
    )
    root_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={'identifier': 'root',
              'directory_path': '/'
              },
        status_code=200
    )
    try:
        validator = DirectoryValidation(client)
        validator.is_valid_for_file(FILE_METADATA)
    except InvalidMetadataError as exc:
        pytest.fail('test_successful_directory_validation fails: ' +
                    exc.message)
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1


@requests_mock.Mocker()
# pylint: disable=invalid-name
def test_directory_validation_caching_works(mocker):
    """ Two files are contained by same directory. Metax is called only once
    for each directory in tree and hence the directory validation as well.
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
    first_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={'identifier': 'first_par',
              'directory_path': '/second_par/first_par',
              'parent_directory': {
                  'identifier': 'second_par'
                  }
              },
        status_code=200
    )
    second_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={'identifier': 'second_par',
              'directory_path': '/second_par',
              'parent_directory': {
                  'identifier': 'root'
                  }
              },
        status_code=200
    )
    root_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={'identifier': 'root',
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
    except InvalidMetadataError as exc:
        pytest.fail('test_successful_directory_validation fails: ' +
                    exc.message)
    # verify that metax is called only once for directories
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1


@requests_mock.Mocker()
# pylint: disable=invalid-name
def test_successful_directory_validation_fails(mocker):
    """Directory validation of /second_par/first_par/file1. The root directory
    is missing the `directory_path` attribute
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
    first_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/first_par',
        json={'identifier': 'first_par',
              'directory_path': '/second_par/first_par',
              'parent_directory': {
                  'identifier': 'second_par'
                  }
              },
        status_code=200
    )
    second_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/second_par',
        json={'identifier': 'second_par',
              'directory_path': '/second_par',
              'parent_directory': {
                  'identifier': 'root'
                  }
              },
        status_code=200
    )
    root_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/root',
        json={'identifier': 'root',
              },
        status_code=200
    )
    with pytest.raises(InvalidMetadataError) as exc_info:
        validator = DirectoryValidation(client)
        validator.is_valid_for_file(FILE_METADATA)
    with open("tests/data/errors/message1.txt") as message:
        assert str(exc_info.value) == message.read()
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1
    assert root_dir_adapter.call_count == 1
