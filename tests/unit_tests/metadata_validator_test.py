"""Tests for :mod:`siptools_research.metadata_validator` module"""
import copy

import pytest
import lxml.etree

from metax_access import Metax
import requests_mock

import siptools_research
from siptools_research import validate_metadata
import siptools_research.metadata_validator as metadata_validator
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
def test_validate_metadata():
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.

    :returns: ``None``
    """
    assert validate_metadata(
        'validate_metadata_test_dataset', tests.conftest.UNIT_TEST_CONFIG_FILE
    )


@pytest.mark.usefixtures('testmetax')
@pytest.mark.parametrize("language", ["en", "fi", "enfi"])
def test_validate_metadata_languages(language, monkeypatch):
    """Test that validate_metadata function returns ``True`` when English,
    Finnish, or both translations are provided.

    :returns: ``None``
    """
    # Datacite validation is patched to only test dataset schema validation.
    # Datacite validation is tested in test_validate_metadata.
    monkeypatch.setattr(
        metadata_validator, "_validate_datacite",
        lambda dataset_id, client: None
    )
    assert validate_metadata(
        'validate_metadata_%s' % language, tests.conftest.UNIT_TEST_CONFIG_FILE
    )


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_language_missing(monkeypatch):
    """Test that metadata validation fails if localization is missing on a
    required field.
    """
    monkeypatch.setattr(
        metadata_validator, "_validate_datacite",
        lambda dataset_id, client: None
    )
    with pytest.raises(InvalidMetadataError) as error:
        validate_metadata(
            'validate_metadata_localization_missing',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    field = "'research_dataset/provenance/description'"
    assert str(error.value) == "No localization provided in field: %s" % field


@pytest.mark.usefixtures('testmetax')
def test_validate_metadata_invalid():
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('validate_metadata_test_dataset_invalid_metadata',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith("'contract' is a required property")


@pytest.mark.usefixtures('testmetax')
@pytest.mark.parametrize('format_version', ["1.0", ""])
def test_validate_invalid_file_type(format_version):
    """Test that validate_metadata function raises exception with correct error
    message for unsupported file type.

    :returns: ``None``
    """
    # Try to validate dataset with a file that has an unsupported file_format
    with pytest.raises(InvalidMetadataError) as error:
        validate_metadata('validate_invalid_file_type_%s' % format_version,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    message = (
        "Validation error in file path/to/file:"
        " Incorrect file format: application/unsupported"
    )
    if format_version:
        message += ", version 1.0"

    assert error.value.message == message


@pytest.mark.usefixtures('testmetax')
# pylint: disable=invalid-name
def test_validate_metadata_invalid_contract_metadata():
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'validate_metadata_test_dataset_invalid_contract_metadata',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    assert exc_info.value.message.startswith(
        "'name' is a required property\n\nFailed validating 'required' in "
        "schema"
    )


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_invalid_file_path():
    """Test that validate_metadata function raises exception if some of the
    file paths point outside SIP.

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('validate_metadata_test_dataset_invalid_file_path',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    assert exception_info.value.message \
        == ("The file path of file pid:urn:invalidpath is invalid: "
            "../../file_in_invalid_path")


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_missing_xml():
    """Test that validate_metadata function raises exception if dataset
    contains image file but not XML metadata.

    :returns: ``None``
    """
    with pytest.raises(InvalidMetadataError) as exc:
        validate_metadata('validate_metadata_test_dataset_metadata_missing',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert exc.value[0] == ("Missing technical metadata XML for file: "
                            "pid:urn:validate_metadata_test_image")


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_audiovideo():
    """Test that validate_metadata function validates AudioMD and VideoMD
    metadata.

    :returns: ``None``
    """
    assert validate_metadata(
        'validate_metadata_test_dataset_audio_video_metadata',
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_invalid_audiomd():
    """Test that validate_metadata function raises exception if AudioMD is
    invalid (missing required audiomd:duration element).

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('validate_metadata_test_dataset_invalid_audiomd',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        "Technical metadata XML of file pid:urn:testaudio is invalid: Element "
        "'audiomd:duration' is required in element 'amd:audioInfo'."
    )


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_corrupted_mix():
    """Test that validate_metadata function raises exception if MIX metadata in
    Metax is corrupted (invalid XML).

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'validate_metadata_test_dataset_corrupted_mix',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        'Technical metadata XML of file pid:urn:testimage is invalid: '
        'Namespace prefix mix on mix is not defined, line 2, column 1'
    )


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_invalid_datacite():
    """Test that validate_metadata function raises exception with correct error
    message for invalid datacite where required attribute identifier is
    missing.

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'validate_metadata_test_dataset_invalid_datacite',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        "Datacite metadata is invalid: Element "
        "'{http://datacite.org/schema/kernel-4}resource': Missing child "
        "element(s)."
    )


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
# pylint: disable=invalid-name
def test_validate_metadata_corrupted_datacite():
    """Test that validate_metadata function raises exception with correct error
    message for corrupted datacite XML.

    :returns: ``None``
    """
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'validate_metadata_test_dataset_corrupted_datacite',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        "Datacite metadata is invalid: Couldn't find end of Start Tag "
        "resource line 1, line 2, column 1"
    )


@pytest.mark.usefixtures('mock_filetype_conf')
@requests_mock.Mocker()
# pylint: disable=invalid-name
def test_validate_metadata_publisher_missing(mocker):
    """Test that validate_metadata function raises exception with correct error
    message if Metax fails to generate datacite for dataset that is missing
    `publisher` attribute.

    :returns: ``None``
    """
    # Mock contract metadata request
    mocker.get(
        tests.conftest.METAX_URL
        + '/contracts/urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd',
        json={
            'contract_json': {
                'identifier': '1',
                'organization': {
                    'name': 'test organization'
                }
            }
        },
        status_code=200
    )

    # Mock set_preservation_identifier API request
    mocker.post(
        tests.conftest.METAX_RPC_URL + '/datasets/set_preservation_identifier'
        '?identifier=validate_metadata_test_dataset_publisher_missing',
        text='foobar'
    )

    # Mock datacite request response. Mocked response has status code 400, and
    # response body contains error information.
    response = \
        {
            "detail": "Dataset does not have a publisher (field: "
                      "research_dataset.publisher), which is a required value "
                      "for datacite format",
            "error_identifier": "2019-03-28T12:39:01-f0a7e3ae"
        }
    mocker.get(
        tests.conftest.METAX_URL + '/datasets/validate_metadata_test_dataset_'
                                   'publisher_missing?dataset_format=datacite',
        json=response,
        status_code=400
    )

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'validate_metadata_test_dataset_publisher_missing',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )

    # Check exception message
    exc = exc_info.value
    assert exc.message == (
        "Datacite metadata is invalid: Dataset does not have a publisher "
        "(field: research_dataset.publisher), which is a required value for "
        "datacite format"
    )


@pytest.mark.usefixtures('mock_filetype_conf')
@requests_mock.Mocker()
@pytest.mark.noautofixt
def test_validate_file_metadata(mocker):
    """Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory
    in dataset.directories property.

    :returns: ``None``
    """
    dataset = {
        'identifier': 'dataset_identifier',
        'research_dataset': {
            'files': [],
            'directories': [{'identifier': 'root_dir'}]
        }
    }

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
    first_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/first_par_dir',
        json={'identifier': 'first_par_dir',
              'parent_directory': {
                  'identifier': 'second_par_dir'
                  }
              },
        status_code=200
    )
    second_par_dir_adapter = mocker.get(
        tests.conftest.METAX_URL + '/directories/second_par_dir',
        json={'identifier': 'second_par_dir',
              'parent_directory': {
                  'identifier': 'root_dir'
                  }
              },
        status_code=200
    )
    files_adapter = mocker.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier/files',
        json=[file_1, file_2],
        status_code=200
    )
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )
    # pylint: disable=protected-access
    siptools_research.metadata_validator._validate_file_metadata(
        dataset,
        client, configuration
    )
    assert files_adapter.call_count == 1
    # verify that dataset directory caching works
    assert first_par_dir_adapter.call_count == 1
    assert second_par_dir_adapter.call_count == 1


@pytest.mark.usefixtures('testmetax')
def test_validate_file_metadata_invalid_metadata():
    """Check that ``_validate_file_metadata`` raises exceptions with
    descriptive error messages.

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

    with pytest.raises(InvalidMetadataError) as exc_info:
        # pylint: disable=protected-access
        siptools_research.metadata_validator._validate_file_metadata(
            {
                'identifier': 'validate_metadata_test_dataset_missing_'
                'file_format'
            },
            client, configuration
        )

    assert exc_info.value.message.startswith(
        "Validation error in metadata of path/to/file: 'file_format' is"
        " a required property\n\n"
        "Failed validating 'required' in schema['properties']"
        "['file_characteristics']:\n"
    )


def test_validate_xml_file_metadata():
    """Test that _validate_xml_file_metadata function raises exception with
    readable error message when validated XML contains multiple errors.

    :returns: ``None``
    """
    xml = lxml.etree.parse('tests/data/invalid_audiomd.xml')
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_with_schematron(
            'audio', xml, 'foo'
        )

    assert exception_info.value.message == (
        "Technical metadata XML of file foo is invalid: The following errors "
        "were detected:\n\n"
        "1. Element 'audiomd:samplingFrequency' is required in element "
        "'amd:fileData'.\n"
        "2. Element 'audiomd:duration' is required in element 'amd:audioInfo'."
    )


@pytest.mark.usefixtures('testmetax')
def test_validate_datacite():
    """Test that _validate_datacite function raises exception with readable
    error message when datacite XML contains multiple errors.

    :returns: ``None``
    """

    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    metax_client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )

    # Validate datacite
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_datacite(
            'validate_metadata_test_dataset_very_invalid_datacite',
            metax_client
        )

    # Check error message
    assert exception_info.value.message == (
        "Datacite metadata is invalid: The following errors were detected:\n\n"
        "1. Element '{http://datacite.org/schema/kernel-4}resourceType', "
        "attribute 'resourceTypeGeneral': [facet 'enumeration'] The value "
        "'INVALID_RESOURCE_TYPE' is not an element of the set {'Audiovisual', "
        "'Collection', 'DataPaper', 'Dataset', 'Event', 'Image', "
        "'InteractiveResource', 'Model', 'PhysicalObject', 'Service', "
        "'Software', 'Sound', 'Text', 'Workflow', 'Other'}.\n"
        "2. Element '{http://datacite.org/schema/kernel-4}resourceType', "
        "attribute 'resourceTypeGeneral': 'INVALID_RESOURCE_TYPE' is not a "
        "valid value of the atomic type "
        "'{http://datacite.org/schema/kernel-4}resourceType'."
    )
