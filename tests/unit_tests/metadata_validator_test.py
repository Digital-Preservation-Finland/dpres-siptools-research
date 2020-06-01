"""Tests for :mod:`siptools_research.metadata_validator` module"""
import copy
import contextlib
import six

import pytest
import lxml.etree
from requests.exceptions import HTTPError

from metax_access import Metax

import siptools_research
from siptools_research import validate_metadata
from siptools_research.metadata_validator import _validate_dataset_metadata
import siptools_research.metadata_validator as metadata_validator
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest
from tests.conftest import mock_metax_dataset
from tests.metax_data.datasets import BASE_DATASET, BASE_DATACITE
from tests.metax_data.files import (BASE_FILE,
                                    TXT_FILE,
                                    TIFF_FILE,
                                    BASE_AUDIO_MD,
                                    BASE_VIDEO_MD)
from tests.metax_data.contracts import BASE_CONTRACT


@contextlib.contextmanager
def does_not_raise():
    """Dummy context manager that complements pytest.raises when no error is
    excepted."""
    yield


def get_bad_audiomd():
    """Creates and return invalid audio metadata xml.
    :returns: Audio MD as string
    """
    root = copy.deepcopy(BASE_AUDIO_MD)
    element = root.xpath('/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/'
                         'mets:xmlData/amd:AUDIOMD/amd:audioInfo/amd:duration',
                         namespaces={'mets': "http://www.loc.gov/METS/",
                                     'amd': "http://www.loc.gov/audioMD/"})
    element[0].getparent().remove(element[0])
    return lxml.etree.tostring(root, pretty_print=True)


def get_invalid_datacite():
    """Creates and returns invalid datacite.
    :returns: Datacite as string
    """
    root = copy.deepcopy(BASE_DATACITE)
    element = root.xpath(
        '/ns:resource/ns:identifier',
        namespaces={'ns': "http://datacite.org/schema/kernel-4"}
    )
    element[0].getparent().remove(element[0])
    return lxml.etree.tostring(root, pretty_print=True)


def get_very_invalid_datacite():
    """Creates and returns very invalid datacite.
    :returns: Datacite as string
    """
    root = copy.deepcopy(BASE_DATACITE)
    element = root.xpath(
        '/ns:resource/ns:resourceType',
        namespaces={'ns': "http://datacite.org/schema/kernel-4"}
    )
    element[0].attrib['resourceTypeGeneral'] = 'INVALID_RESOURCE_TYPE'
    return lxml.etree.tostring(root, pretty_print=True)


def test_validate_metadata(requests_mock):
    """Test that validate_metadata function returns ``True`` for a valid
    dataset and sets preservation state in Metax.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = "pid:urn:1"
    files[1]['identifier'] = "pid:urn:2"
    mock_metax_dataset(requests_mock, files=files)

    assert validate_metadata(
        'dataset_identifier',
        tests.conftest.UNIT_TEST_CONFIG_FILE,
        set_preservation_state=True
    )

    # Preservation state should be patched
    assert requests_mock.last_request.json()['preservation_state'] == 70
    assert requests_mock.last_request.json()['preservation_description'] \
        == 'Metadata passed validation'


@pytest.mark.parametrize(
    ("translations", "expectation"),
    (
        (
            {"en": "Something in english"},
            does_not_raise()),
        (
            {"fi": "Jotain suomeksi"},
            does_not_raise()
        ),
        (
            {"en": "Something in english", "fi": "Jotain suomeksi"},
            does_not_raise()
        ),
        (
            {"foo": "Something in invalid language"},
            pytest.raises(InvalidMetadataError,
                          match=("Invalid language code: 'foo' in field: "
                                 "'research_dataset/provenance/description"))
        ),
        (
            {"foo": "Something in invalid language"},
            pytest.raises(InvalidMetadataError,
                          match=("Invalid language code: 'foo' in field: "
                                 "'research_dataset/provenance/description"))
        ),
        (
            {},
            pytest.raises(InvalidMetadataError,
                          match=("No localization provided in field: "
                                 "'research_dataset/provenance/description'"))
        )
    )
)
# pylint: disable=invalid-name
def test_validate_metadata_languages(translations, expectation, requests_mock):
    """Test that validate_metadata function when one one of the localized
    fields has different translations. Invalid translations should raise
    exception.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['research_dataset']['provenance'][0]['description'] = translations
    mock_metax_dataset(requests_mock, dataset=dataset)

    with expectation:
        assert validate_metadata('dataset_identifier',
                                 tests.conftest.UNIT_TEST_CONFIG_FILE,
                                 set_preservation_state=True)


# pylint: disable=invalid-name
def test_validate_metadata_invalid(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset. Also check that preservation state is reported
    to Metax.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )

    # Try to validate invalid dataset
    excepted_error = "'contract' is a required property"
    with pytest.raises(InvalidMetadataError, match=excepted_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Preservation state and error message should be patched
    assert adapter.last_request.json()['preservation_state'] == 40
    assert adapter.last_request.json()['preservation_description'].startswith(
        "Metadata did not pass validation: {}".format(excepted_error)
    )


# pylint: disable=invalid-name
def test_validate_preservation_identifier():
    """Test that _validate_dataset_metadata function handles missing
    preservation_identifier correctly.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['preservation_identifier']

    # Validation with dummy DOI should not raise an exception
    _validate_dataset_metadata(dataset, dummy_doi="true")

    # Validation without dummy DOI should raise an exception
    with pytest.raises(InvalidMetadataError) as exc_info:
        _validate_dataset_metadata(dataset)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "'preservation_identifier' is a required property"
    )


@pytest.mark.parametrize(
    ('file_characteristics',
     'version_info'),
    [
        ({'file_format': 'application/unsupported', 'format_version': '1.0'},
         ", version 1.0"),
        ({'file_format': 'application/unsupported'},
         "")
    ]
)
# pylint: disable=invalid-name
def test_validate_invalid_file_type(file_characteristics, version_info,
                                    requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for unsupported file type.

    :param file_characteristics: file characteristics dict in file metadata
    :param version_info: excepted version information in exception message
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    unsupported_file = copy.deepcopy(BASE_FILE)
    unsupported_file['file_characteristics'] = file_characteristics
    mock_metax_dataset(requests_mock, files=[unsupported_file])

    # Try to validate dataset with a file that has an unsupported file_format
    with pytest.raises(InvalidMetadataError) as error:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    excepted_exception = (
        "Validation error in file path/to/file: Incorrect file format: "
        "application/unsupported{}".format(version_info)
    )
    assert str(error.value) == excepted_exception


# pylint: disable=invalid-name
def test_validate_metadata_invalid_contract_metadata(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    invalid_contract = copy.deepcopy(BASE_CONTRACT)
    del invalid_contract['contract_json']['organization']['name']
    mock_metax_dataset(requests_mock, contract=invalid_contract)

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True
        )

    # Check exception message
    assert str(exc_info.value).startswith(
        "'name' is a required property\n\n"
        "Failed validating 'required' in schema"
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_file_path(requests_mock):
    """Test that validate_metadata function raises exception if some of the
    file paths point outside SIP.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    invalid_file = copy.deepcopy(TXT_FILE)
    invalid_file['file_path'] = "../../file_in_invalid_path"
    mock_metax_dataset(requests_mock, files=[invalid_file])

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    assert str(exception_info.value) \
        == ("The file path of file pid:urn:identifier is invalid: "
            "../../file_in_invalid_path")


# pylint: disable=invalid-name
def test_validate_metadata_missing_xml(requests_mock):
    """Test that validate_metadata function raises exception if dataset
    contains image file but not XML metadata.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock, files=[TIFF_FILE])

    with pytest.raises(InvalidMetadataError) as exc:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    assert str(exc.value) == ("Missing technical metadata XML for file: "
                              "pid:urn:identifier")


# pylint: disable=invalid-name
@pytest.mark.parametrize(
    ('file_format', 'xml_namespace', 'xml'),
    (
        ('audio/mp4', "http://www.loc.gov/audioMD/", BASE_AUDIO_MD),
        ('video/mp4', "http://www.loc.gov/videoMD/", BASE_VIDEO_MD)
    )
)
def test_validate_metadata_audiovideo(requests_mock,
                                      file_format,
                                      xml_namespace,
                                      xml):
    """Test that validate_metadata function validates AudioMD and VideoMD
    metadata.

    :param requests_mock: Mocker object
    :param file_format: file mimetype
    :param xml_namespace: namespace for of technical metadata
    :param xml: techincal metada as xml object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata['file_characteristics'] = {"file_format": file_format}
    mock_metax_dataset(requests_mock, files=[file_metadata])

    requests_mock.get("https://metaksi/rest/v1/files/{}/xml"
                      .format(file_metadata['identifier']),
                      json=[xml_namespace])
    requests_mock.get(
        "https://metaksi/rest/v1/files/{}/xml?"
        "namespace={}".format(file_metadata['identifier'], xml_namespace),
        content=six.binary_type(lxml.etree.tostring(xml))
    )

    assert validate_metadata('dataset_identifier',
                             tests.conftest.UNIT_TEST_CONFIG_FILE,
                             set_preservation_state=True)


# pylint: disable=invalid-name
def test_validate_metadata_invalid_audiomd(requests_mock):
    """Test that validate_metadata function raises exception if AudioMD is
    invalid (missing required audiomd:duration element).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    audio_file = copy.deepcopy(BASE_FILE)
    audio_file['file_characteristics'] = {"file_format": "audio/mp4"}
    mock_metax_dataset(requests_mock, files=[audio_file])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      content=get_bad_audiomd())

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Technical metadata XML of file pid:urn:identifier is invalid: Element"
        " 'audiomd:duration' is required in element 'amd:audioInfo'."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_mix(requests_mock):
    """Test that validate_metadata function raises exception if MIX metadata in
    Metax is corrupted (invalid XML).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock, files=[TIFF_FILE])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        'Technical metadata XML of file pid:urn:identifier is invalid: '
        'Namespace prefix mix on mix is not defined, line 2, column 1'
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid datacite where required attribute identifier is
    missing.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Datacite metadata is invalid: Element "
        "'{http://datacite.org/schema/kernel-4}resource': Missing child "
        "element(s)."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for corrupted datacite XML.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text="<resource\n")

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Datacite metadata is invalid: Couldn't find end of Start Tag "
        "resource line 1, line 2, column 1"
    )


# pylint: disable=invalid-name
def test_validate_metadata_datacite_bad_request(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message if Metax fails to generate datacite for dataset, for example when
    `publisher` attribute is missing.

    :param requests_mock: Mocker object
    :returns: ``None``
    """

    mock_metax_dataset(requests_mock)

    # Mock datacite request response. Mocked response has status code 400, and
    # response body contains error information.
    requests_mock.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier?'
        'dataset_format=datacite',
        json={"detail": "Bad request"},
        status_code=400
    )

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True
        )

    # Check exception message
    exc = exc_info.value
    assert str(exc) == "Datacite metadata is invalid: Bad request"


def test_validate_file_metadata(requests_mock):
    """Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory
    in dataset.directories property.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['research_dataset']['directories'] = [{'identifier': 'root_dir'}]
    file_1 = copy.deepcopy(TXT_FILE)
    file_1['identifier'] = 'file_identifier1'
    file_2 = copy.deepcopy(TXT_FILE)
    file_2['identifier'] = 'file_identifier2'
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/pid:urn:dir:wf1',
        json={'identifier': 'first_par_dir',
              'directory_path': '',
              'parent_directory': {'identifier': 'second_par_dir'}},
        status_code=200
    )
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/second_par_dir',
        json={'identifier': 'second_par_dir',
              'directory_path': '',
              'parent_directory': {'identifier': 'root_dir'}},
        status_code=200
    )
    requests_mock.get(
        tests.conftest.METAX_URL + '/directories/root_dir',
        json={'identifier': 'second_par_dir', 'directory_path': '/'},
        status_code=200
    )
    files_adapter = requests_mock.get(
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


# pylint: disable=invalid-name
def test_validate_file_metadata_invalid_metadata(requests_mock):
    """Check that ``_validate_file_metadata`` raises exceptions with
    descriptive error messages.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata['file_characteristics'] = {
        "file_created": "2014-01-17T08:19:31Z"
    }
    mock_metax_dataset(requests_mock, files=[file_metadata])

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
                'identifier': 'dataset_identifier'
            },
            client, configuration
        )

    assert str(exc_info.value).startswith(
        "Validation error in metadata of path/to/file: 'file_format' is"
        " a required property\n\n"
        "Failed validating 'required' in schema['properties']"
        "['file_characteristics']:\n"
    )


def test_validate_xml_file_metadata():
    """Test that _validate_xml_file_metadata function raises exception with
    readable error message when validated XML contains multiple errors.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    xml = lxml.etree.parse('tests/data/invalid_audiomd.xml')
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_with_schematron(
            'audio', xml, 'foo'
        )

    assert str(exception_info.value) == (
        "Technical metadata XML of file foo is invalid: The following errors "
        "were detected:\n\n"
        "1. Element 'audiomd:samplingFrequency' is required in element "
        "'amd:fileData'.\n"
        "2. Element 'audiomd:duration' is required in element 'amd:audioInfo'."
    )


def test_validate_datacite(requests_mock):
    """Test that _validate_datacite function raises exception with readable
    error message when datacite XML contains multiple errors.

    :param requests_mock: Mocker object
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

    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_very_invalid_datacite())

    # Validate datacite
    # pylint: disable=protected-access
    with pytest.raises(InvalidMetadataError) as exception_info:
        metadata_validator._validate_datacite(
            'dataset_identifier',
            metax_client
        )

    # Check error message
    assert str(exception_info.value).startswith(
        "Datacite metadata is invalid:"
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_directory_metadata(requests_mock):
    """Test that validate_metadata function raises exception if directory
    metadata is not valid (directory_path attribute is missing).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(TXT_FILE)
    mock_metax_dataset(requests_mock, files=[file_metadata])
    requests_mock.get("https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
                      json={"identifier": "pid:urn:dir:wf1"})

    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    assert str(exception_info.value).startswith(
        "Validation error in metadata of pid:urn:dir:wf1: 'directory_path' is"
        " a required property")


# pylint: disable=invalid-name
def test_validate_metadata_http_error_raised(requests_mock):
    """Test that validate_metadata does not write the HTTPError to
    dataset's preservation_state attribute in Metax when HTTPError occurs 

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    mock_metax_dataset(requests_mock)
    requests_mock.get(
        'https://metaksi/rest/v1/datasets/dataset_identifier/files',
        status_code=500,
        reason='Something not to be shown to user'
    )

    with pytest.raises(HTTPError):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Assert preservation state is set correctly
    assert requests_mock.last_request.method == "PATCH"
    body = requests_mock.last_request.json()
    assert body['preservation_state'] == 50
    # TODO: The message of HTTPErrors will be different in newer versions of
    # requests library (this test works with version 2.6 which is available in
    # centos7 repositories).
    assert body['preservation_description'] == "System error"
