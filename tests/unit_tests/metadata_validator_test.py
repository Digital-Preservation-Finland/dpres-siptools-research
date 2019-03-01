"""Tests for :mod:`siptools_research.metadata_validator` module"""

import pytest
import tests.conftest
import lxml.etree
import siptools_research
from siptools_research import validate_metadata
import siptools_research.metadata_validator
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
from metax_access import Metax


@pytest.mark.usefixtures('testmetax', 'mock_filetype_conf')
def test_validate_metadata():
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.

    :returns: ``None``
    """
    assert validate_metadata('validate_metadata_test_dataset',
                             tests.conftest.UNIT_TEST_CONFIG_FILE) is True


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
    assert exc_info.value.message \
        == ("'name' is a required property\n"
            "\n"
            "Failed validating 'required' in schema['properties']"
            "['contract_json']['properties']['organization']:\n"
            "    {'properties': {'name': {'type': 'string'}},\n"
            "     'required': ['name'],\n"
            "     'type': 'object'}\n"
            "\n"
            "On instance['contract_json']['organization']:\n"
            "    {u'foo': u'bar'}")


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
        tests.conftest.UNIT_TEST_CONFIG_FILE) is True


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
        validate_metadata('validate_metadata_test_dataset_corrupted_mix',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

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
        validate_metadata('validate_metadata_test_dataset_invalid_datacite',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        'Datacite metadata is invalid: Element '
        '\'{http://datacite.org/schema/kernel-4}resource\': Missing child '
        'element(s).'
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
        validate_metadata('validate_metadata_test_dataset_corrupted_datacite',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check exception message
    exc = exc_info.value
    assert exc.message.startswith(
        "Datacite metadata is invalid: Couldn't find end of Start Tag "
        "resource line 1, line 2, column 1"
    )


@pytest.mark.usefixtures('testmetax')
def test_validate_file_metadata():
    """Check that ``_validate_file_metadata`` raises exceptions with
    descriptive error messages.

    :returns: ``None``
    """
    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(configuration.get('metax_url'),
                   configuration.get('metax_user'),
                   configuration.get('metax_password'))

    with pytest.raises(InvalidMetadataError) as exc_info:
        # pylint: disable=protected-access
        siptools_research.metadata_validator._validate_file_metadata(
            'validate_metadata_test_dataset_missing_file_format', client
        )

    assert exc_info.value.message \
        == ("Validation error in metadata of path/to/file1: 'file_format' is"
            " a required property\n"
            "\n"
            "Failed validating 'required' in schema['properties']"
            "['file_characteristics']:\n"
            "    {'properties': {'file_encoding': {'enum': ['ISO-8859-15',\n"
            "                                               'UTF-8',\n"
            "                                               'UTF-16',\n"
            "                                               'UTF-32'],\n"
            "                                      'type': 'string'}},\n"
            "     'required': ['file_format'],\n"
            "     'type': 'object'}\n"
            "\n"
            "On instance['file_characteristics']:\n"
            "    {u'file_created': u'2014-01-17T08:19:31Z'}")


def test_validate_xml_file_metadata():
    """Test that _validate_xml_file_metadata function raises exception with
    readable error message when validated XML contains multiple errors.

    :returns: ``None``
    """
    xml = lxml.etree.parse('tests/data/invalid_audiomd.xml')
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        siptools_research.metadata_validator._validate_with_schematron(
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
    metax_client = Metax(configuration.get('metax_url'),
                         configuration.get('metax_user'),
                         configuration.get('metax_password'))

    # Validate datacite
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        siptools_research.metadata_validator._validate_datacite(
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
