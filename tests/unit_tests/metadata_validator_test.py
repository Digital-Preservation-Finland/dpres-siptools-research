"""Tests for :mod:`siptools_research.metadata_validator` module"""

import pytest
import tests.conftest
import siptools_research
from siptools_research import validate_metadata
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

    assert exc.value[0] == \
        "Missing XML metadata for file: pid:urn:validate_metadata_test_image"


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
    assert exc.message.startswith('Datacite (id=validate_metadata_test_data' +
                                  'set_invalid_datacite) validation failed')


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
