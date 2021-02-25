"""Tests for :mod:`siptools_research.metadata_validator` module."""
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
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.exceptions import InvalidFileMetadataError
from siptools_research.exceptions import InvalidContractMetadataError
from siptools_research.config import Configuration
from tests.metax_data.datasets import BASE_DATASET, BASE_DATACITE
from tests.metax_data.files import (BASE_FILE,
                                    TXT_FILE,
                                    TIFF_FILE,
                                    BASE_ADDML_MD,
                                    BASE_AUDIO_MD,
                                    BASE_VIDEO_MD)
from tests.metax_data.contracts import BASE_CONTRACT
import tests.utils


@contextlib.contextmanager
def does_not_raise():
    """Yield nothing.

    This is a dummy context manager that complements pytest.raises when no
    error is expected.
    """
    yield


def get_bad_audiomd():
    """Create invalid audio metadata xml.

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
    """Create invalid datacite.

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
    """Create very invalid datacite.

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
    """Test validate_metadata.

    Function should return ``True`` for a valid dataset.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = "pid:urn:1"
    files[1]['identifier'] = "pid:urn:2"
    tests.utils.add_metax_dataset(requests_mock, files=files)

    assert validate_metadata('dataset_identifier',
                             tests.conftest.UNIT_TEST_CONFIG_FILE)


def test_validate_metadata_missing_file(requests_mock):
    """Test validate_metadata with an empty dataset.

    Function should raise InvalidDatasetMetadataError for datasets, which do
    not contain any files.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_dataset(requests_mock)
    expected_error = "Dataset must contain at least one file"

    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE
        )


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
            pytest.raises(InvalidDatasetMetadataError,
                          match=("Invalid language code: 'foo' in field: "
                                 "'research_dataset/provenance/description"))
        ),
        (
            {},
            pytest.raises(InvalidDatasetMetadataError,
                          match=("No localization provided in field: "
                                 "'research_dataset/provenance/description'"))
        )
    )
)
# pylint: disable=invalid-name
def test_validate_metadata_languages(translations, expectation, requests_mock):
    """Test validate_metadata.

    Function should raise exception when one of the localized
    fields has invalid values.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset_file = copy.deepcopy(TXT_FILE)
    dataset['research_dataset']['provenance'][0]['description'] = translations
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=[dataset_file])

    with expectation:
        assert validate_metadata('dataset_identifier',
                                 tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_invalid(requests_mock):
    """Test validate_metadata.

    Function should raise exception with correct error message for invalid
    dataset.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)

    # Try to validate invalid dataset
    expected_error = "'contract' is a required property"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_preservation_identifier():
    """Test _validate_dataset_metadata.

    Function should raise exception if preservation_identifier is missing from
    metadata.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['preservation_identifier']

    # Validation with dummy DOI should not raise an exception
    _validate_dataset_metadata(dataset, dummy_doi="true")

    # Validation without dummy DOI should raise an exception
    expected_error = "'preservation_identifier' is a required property"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        _validate_dataset_metadata(dataset)


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
    """Test validate_metadata.

    Function should raise exception with correct error message for unsupported
    file type.

    :param file_characteristics: file characteristics dict in file metadata
    :param version_info: expected version information in exception message
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    unsupported_file = copy.deepcopy(BASE_FILE)
    unsupported_file['file_characteristics'] = file_characteristics
    tests.utils.add_metax_dataset(requests_mock, files=[unsupported_file])

    # Try to validate dataset with a file that has an unsupported file_format
    expected_error = (
        "Validation error in file path/to/file: Incorrect file format: "
        "application/unsupported{}".format(version_info)
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_invalid_contract_metadata(requests_mock):
    """Test validate_metadata.

    Function raises exception with correct error message for invalid dataset.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    invalid_contract = copy.deepcopy(BASE_CONTRACT)
    del invalid_contract['contract_json']['organization']['name']
    tests.utils.add_metax_dataset(requests_mock, contract=invalid_contract)

    # Try to validate invalid dataset
    expected_error = ("'name' is a required property\n\n"
                      "Failed validating 'required' in schema")
    with pytest.raises(InvalidContractMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_invalid_file_path(requests_mock):
    """Test validate_metadata.

    Function should raise exception if some of the
    file paths point outside SIP.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    invalid_file = copy.deepcopy(TXT_FILE)
    invalid_file['file_path'] = "../../file_in_invalid_path"
    tests.utils.add_metax_dataset(requests_mock, files=[invalid_file])

    # Try to validate invalid dataset
    expected_error = ("The file path of file pid:urn:identifier is invalid: "
                      "../../file_in_invalid_path")
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_missing_xml(requests_mock):
    """Test validate_metadata.

    Function should raise exception if dataset contains image file but not XML
    metadata.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_dataset(requests_mock, files=[TIFF_FILE])

    expected_error \
        = "Missing technical metadata XML for file: pid:urn:identifier"
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
@pytest.mark.parametrize(
    ('file_format', 'xml_namespace', 'xml'),
    (
        (
            'text/csv',
            "http://www.arkivverket.no/standarder/addml", BASE_ADDML_MD
        ),
        ('audio/mp4', "http://www.loc.gov/audioMD/", BASE_AUDIO_MD),
        ('video/mp4', "http://www.loc.gov/videoMD/", BASE_VIDEO_MD)
    )
)
def test_validate_metadata_multiple_formats(
        requests_mock, file_format, xml_namespace, xml):
    """Test validate_metadata.

    Function validates different types of technical metadata.

    :param requests_mock: Mocker object
    :param file_format: file mimetype
    :param xml_namespace: namespace for of technical metadata
    :param xml: techincal metada as xml object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata['file_characteristics'] = {"file_format": file_format}
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])

    requests_mock.get("https://metaksi/rest/v1/files/{}/xml"
                      .format(file_metadata['identifier']),
                      json=[xml_namespace])
    requests_mock.get(
        "https://metaksi/rest/v1/files/{}/xml?"
        "namespace={}".format(file_metadata['identifier'], xml_namespace),
        content=six.binary_type(lxml.etree.tostring(xml))
    )

    assert validate_metadata('dataset_identifier',
                             tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_invalid_audiomd(requests_mock):
    """Test validate_metadata.

    Function should raise exception if AudioMD is
    invalid (missing required audiomd:duration element).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    audio_file = copy.deepcopy(BASE_FILE)
    audio_file['file_characteristics'] = {"file_format": "audio/mp4"}
    tests.utils.add_metax_dataset(requests_mock, files=[audio_file])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      content=get_bad_audiomd())

    # Try to validate invalid dataset
    expected_error = (
        "Technical metadata XML of file is invalid: Element"
        " 'audiomd:duration' is required in element 'amd:audioInfo'."
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_mix(requests_mock):
    """Test validate_metadata.

    Function shoudl raise exception if MIX metadata in
    Metax is corrupted (invalid XML).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_dataset(requests_mock, files=[TIFF_FILE])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:identifier/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")

    # Try to validate invalid dataset
    expected_error = (
        'Technical metadata XML of file is invalid: '
        'Namespace prefix mix on mix is not defined, line 2, column 1'
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_invalid_datacite(requests_mock):
    """Test validate_metadata.

    Function should raise exception with correct error message for invalid
    datacite where required attribute identifier is missing.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset_file = copy.deepcopy(TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[dataset_file])
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())

    # Try to validate invalid dataset
    with pytest.raises(InvalidDatasetMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check error message
    assert str(exception_info.value).startswith(
        "Datacite metadata is invalid: Element "
        "'{http://datacite.org/schema/kernel-4}resource': Missing child "
        "element(s)."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_datacite(requests_mock):
    """Test validate_metadata.

    Function should raise exception  if datacite XML is corrupted.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset_file = copy.deepcopy(TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[dataset_file])
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text="<resource\n")

    # Try to validate invalid dataset
    expected_error \
        = "Couldn't find end of Start Tag resource line 1, line 2, column 1"
    with pytest.raises(Exception, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_datacite_bad_request(requests_mock):
    """Test validate_metadata.

    Function should raise exception with correct error message if Metax fails
    to generate datacite for dataset, for example when `publisher` attribute is
    missing.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    dataset_file = copy.deepcopy(TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[dataset_file])

    # Mock datacite request response. Mocked response has status code 400, and
    # response body contains error information.
    requests_mock.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier?'
        'dataset_format=datacite',
        json={"detail": "Bad request"},
        status_code=400
    )

    # Try to validate invalid dataset
    expected_error = "Datacite generation failed: Bad request"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


def test_validate_file_metadata(requests_mock):
    """Test _validate_file_metadata.

    Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory in
    dataset.directories property.

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
    """Test ``_validate_file_metadata``.

    Function should raise exceptions with descriptive error messages.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata['file_characteristics'] = {
        "file_created": "2014-01-17T08:19:31Z"
    }
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])

    # Init metax client
    configuration = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    client = Metax(
        configuration.get('metax_url'),
        configuration.get('metax_user'),
        configuration.get('metax_password'),
        verify=configuration.getboolean('metax_ssl_verification')
    )

    expected_error = (
        "Validation error in metadata of path/to/file: 'file_format' is"
        " a required property\n\nFailed validating 'required' in schema"
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        # pylint: disable=protected-access
        siptools_research.metadata_validator._validate_file_metadata(
            {
                'identifier': 'dataset_identifier'
            },
            client, configuration
        )


def test_validate_xml_file_metadata():
    """Test _validate_xml_file_metadata.

    Function should raise exception with readable error message when validated
    XML contains multiple errors.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    xml = lxml.etree.parse('tests/data/invalid_audiomd.xml')
    expected_error = (
        "Technical metadata XML of file is invalid: The following errors "
        "were detected:\n\n"
        "1. Element 'audiomd:samplingFrequency' is required in element "
        "'amd:fileData'.\n"
        "2. Element 'audiomd:duration' is required in element 'amd:audioInfo'."
    )
    # pylint: disable=protected-access
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        metadata_validator._validate_with_schematron(
            '/usr/share/dpres-xml-schemas/schematron/mets_audiomd.sch', xml
        )


def test_validate_datacite(requests_mock):
    """Test _validate_datacite.

    Function should raises exception with readable error message when datacite
    XML contains multiple errors.

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

    # Try to validate datacite
    expected_error = "Datacite metadata is invalid:"
    # pylint: disable=protected-access
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        metadata_validator._validate_datacite(
            'dataset_identifier',
            metax_client
        )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_directory_metadata(requests_mock):
    """Test validate_metadata wihth invalid directory metadata.

    Function should raise exception if directory metadata is not valid
    (directory_path attribute is missing).

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    file_metadata = copy.deepcopy(TXT_FILE)
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    requests_mock.get("https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
                      json={"identifier": "pid:urn:dir:wf1"})

    # Try to validate invalid dataset
    expected_error = ("Validation error in metadata of pid:urn:dir:wf1: "
                      "'directory_path' is a required property")
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


# pylint: disable=invalid-name
def test_validate_metadata_http_error_raised(requests_mock):
    """Test validate_metadata.

    Function should raise HTTPError if Metax fails.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_dataset(requests_mock)
    requests_mock.get(
        'https://metaksi/rest/v1/datasets/dataset_identifier/files',
        status_code=500,
        reason='Something not to be shown to user'
    )

    expected_error = '500 Server Error: Something not to be shown to user'
    with pytest.raises(HTTPError, match=expected_error):
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
