"""Tests for :mod:`siptools_research.metadata_validator` module"""
import copy
import contextlib

import pytest
import lxml.etree

from metax_access import Metax

import siptools_research
from siptools_research import validate_metadata
from siptools_research.metadata_validator import _validate_dataset_metadata
import siptools_research.metadata_validator as metadata_validator
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
import tests.conftest
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import BASE_FILE, TXT_FILE
from tests.metax_data.contracts import BASE_CONTRACT

BASE_AUDIO_MD = lxml.etree.parse('tests/data/audiomd_sample.xml')
BASE_VIDEO_MD = lxml.etree.parse('tests/data/audiomd_sample.xml')
BASE_DATACITE = lxml.etree.parse('tests/data/datacite_sample.xml')


@contextlib.contextmanager
def does_not_raise():
    """Dummy context manager that complements pytest.raises when no error is
    excepted."""
    yield


def add_files_to_dataset(files, dataset):
    """Add files to dataset.
    :param: files: file identifier to be added
    :param: dataset
    :returns: ``None``
    """
    for _file in files:
        files = dataset["research_dataset"]["files"]
        files.append({
            "identifier": _file,
            "use_category": {
                "pref_label": {
                    "en": "label2"
                }
            }
        })


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


def mock_metax_get_dir(mocker):
    """Mocks Metax get directory.
    :returns: ``None``
    """
    mocker.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json={"identifier": "pid:urn:dir:wf1", "directory_path": "/access"}
    )


def test_validate_metadata(requests_mock):
    """Test that validate_metadata function returns ``True`` for a valid
    dataset.
    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(TXT_FILE)
    file1['identifier'] = "pid:urn:wf_test_1a_ida"
    file2 = copy.deepcopy(TXT_FILE)
    file2['identifier'] = "pid:urn:wf_test_1b_ida"
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    assert validate_metadata(
        'dataset_identifier', tests.conftest.UNIT_TEST_CONFIG_FILE,
        set_preservation_state=True)
    _assert_metadata_validation_passed(adapter.last_request.json())


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
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get('https://metaksi/rest/v1/datasets/dataset_identifier'
                      '?dataset_format=datacite&dummy_doi=false',
                      content=lxml.etree.tostring(BASE_DATACITE))
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
    requests_mock.patch("https://metaksi/rest/v1/datasets/dataset_identifier")

    with expectation:
        assert validate_metadata('dataset_identifier',
                                 tests.conftest.UNIT_TEST_CONFIG_FILE,
                                 set_preservation_state=True)


# pylint: disable=invalid-name
def test_validate_metadata_invalid(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

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
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith("'contract' is a required property")
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: 'contract' is a required "
        "property\n\nFailed validating 'required' in schema:\n    "
        "{'properties': {'contract': {'description': 'Metadata for")


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
    mock_metax_get_dir(requests_mock)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    unsupported_file = copy.deepcopy(BASE_FILE)
    unsupported_file['file_characteristics'] = file_characteristics
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[unsupported_file]
    )
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )

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

    # Check preservation state posted to Metax
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: {}".format(excepted_exception)
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_contract_metadata(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid dataset.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    contract = copy.deepcopy(BASE_CONTRACT)
    del contract['contract_json']['organization']['name']
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=contract)
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata(
            'dataset_identifier',
            tests.conftest.UNIT_TEST_CONFIG_FILE,
            set_preservation_state=True
        )

    # Check exception message
    assert str(exc_info.value).startswith(
        "'name' is a required property\n\nFailed validating 'required' in "
        "schema"
    )
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: 'name' is a required "
        "property\n\nFailed validating 'required' in schema['properties']"
        "['contract_json']['properties']['organization']:\n    "
        "{'description': 'Organization")


# pylint: disable=invalid-name
def test_validate_metadata_invalid_file_path(requests_mock):
    """Test that validate_metadata function raises exception if some of the
    file paths point outside SIP.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:invalidpath"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file_ = copy.deepcopy(TXT_FILE)
    file_['identifier'] = "pid:urn:invalidpath"
    file_['file_path'] = "../../file_in_invalid_path"
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    assert str(exception_info.value) \
        == ("The file path of file pid:urn:invalidpath is invalid: "
            "../../file_in_invalid_path")
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: The file path of file "
        "pid:urn:invalidpath is invalid: ../../file_in_invalid_path"
    )


# pylint: disable=invalid-name
def test_validate_metadata_missing_xml(requests_mock):
    """Test that validate_metadata function raises exception if dataset
    contains image file but not XML metadata.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:validate_metadata_test_image"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:validate_metadata_test_image"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "image/tiff",
        "format_version": "6.0"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/files/"
                      "pid:urn:validate_metadata_test_image/xml",
                      json={})
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    with pytest.raises(InvalidMetadataError) as exc:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    assert str(exc.value) == ("Missing technical metadata XML for file: "
                              "pid:urn:validate_metadata_test_image")
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Missing technical metadata XML for "
        "file: pid:urn:validate_metadata_test_image"
    )


# pylint: disable=invalid-name
def test_validate_metadata_audiovideo(requests_mock):
    """Test that validate_metadata function validates AudioMD and VideoMD
    metadata.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:891", "pid:urn:892"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(BASE_FILE)
    file1['identifier'] = "pid:urn:891"
    file1['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "audio/mp4"
    }
    file2 = copy.deepcopy(BASE_FILE)
    file2['identifier'] = "pid:urn:892"
    file2['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "video/mp4"
    }
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:891/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:891/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      text=lxml.etree.tostring(BASE_AUDIO_MD))
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:892/xml",
                      json=["http://www.loc.gov/videoMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:892/xml?"
                      "namespace=http://www.loc.gov/videoMD/",
                      text=lxml.etree.tostring(BASE_VIDEO_MD))
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    assert validate_metadata(
        'dataset_identifier',
        tests.conftest.UNIT_TEST_CONFIG_FILE,
        set_preservation_state=True
    )
    _assert_metadata_validation_passed(adapter.last_request.json())


# pylint: disable=invalid-name
def test_validate_metadata_invalid_audiomd(requests_mock):
    """Test that validate_metadata function raises exception if AudioMD is
    invalid (missing required audiomd:duration element).

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:testaudio"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:testaudio"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "audio/mp4"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testaudio/xml",
                      json=["http://www.loc.gov/audioMD/"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testaudio/xml?"
                      "namespace=http://www.loc.gov/audioMD/",
                      content=get_bad_audiomd())
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exc_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    exc = exc_info.value
    assert str(exc).startswith(
        "Technical metadata XML of file pid:urn:testaudio is invalid: Element "
        "'audiomd:duration' is required in element 'amd:audioInfo'."
    )
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Technical metadata XML of file "
        "pid:urn:testaudio is invalid: Element 'audiomd:duration' is required "
        "in element 'amd:audioInfo'."
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_mix(requests_mock):
    """Test that validate_metadata function raises exception if MIX metadata in
    Metax is corrupted (invalid XML).

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:testimage"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:testimage"
    file_['file_characteristics'] = {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "image/tiff",
        "format_version": "6.0"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
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
    assert str(exc).startswith(
        'Technical metadata XML of file pid:urn:testimage is invalid: '
        'Namespace prefix mix on mix is not defined, line 2, column 1'
    )
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Technical metadata XML of file "
        "pid:urn:testimage is invalid: Namespace prefix mix on mix is not "
        "defined, line 2, column 1"
    )


# pylint: disable=invalid-name
def test_validate_metadata_invalid_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for invalid datacite where required attribute identifier is
    missing.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file1 = copy.deepcopy(TXT_FILE)
    file1['identifier'] = "pid:urn:wf_test_1a_ida"
    file2 = copy.deepcopy(TXT_FILE)
    file2['identifier'] = "pid:urn:wf_test_1b_ida"
    files = [file1, file2]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
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
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Datacite metadata is invalid: "
        "Element '{http://datacite.org/schema/kernel-4}resource': Missing "
        "child element(s). Expected is one of "
        "( {http://datacite.org/schema/ker"
    )


# pylint: disable=invalid-name
def test_validate_metadata_corrupted_datacite(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message for corrupted datacite XML.

    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text="<resource\n")
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
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
    assert str(exc).startswith(
        "Datacite metadata is invalid: Couldn't find end of Start Tag "
        "resource line 1, line 2, column 1"
    )
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Datacite metadata is invalid: "
        "Couldn't find end of Start Tag resource line 1, line 2, column 1"
    )


# pylint: disable=invalid-name
def test_validate_metadata_publisher_missing(requests_mock):
    """Test that validate_metadata function raises exception with correct error
    message if Metax fails to generate datacite for dataset that is missing
    `publisher` attribute.

    :returns: ``None``
    """
    mock_metax_get_dir(requests_mock)
    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[]
    )
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      content=get_invalid_datacite())

    # Mock datacite request response. Mocked response has status code 400, and
    # response body contains error information.
    response = \
        {
            "detail": "Dataset does not have a publisher (field: "
                      "research_dataset.publisher), which is a required value "
                      "for datacite format",
            "error_identifier": "2019-03-28T12:39:01-f0a7e3ae"
        }
    requests_mock.get(
        tests.conftest.METAX_URL + '/datasets/dataset_identifier?'
        'dataset_format=datacite',
        json=response,
        status_code=400
    )
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
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
    assert str(exc) == (
        "Datacite metadata is invalid: Dataset does not have a publisher "
        "(field: research_dataset.publisher), which is a required value for "
        "datacite format"
    )
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Datacite metadata is invalid: "
        "Dataset does not have a publisher (field: "
        "research_dataset.publisher), which is a required value for datacite "
        "format"
    )


def test_validate_file_metadata(requests_mock):
    """Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory
    in dataset.directories property.

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
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["pid:urn:wf_test_1a_ida_missing_file_format"],
                         dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    file_ = copy.deepcopy(BASE_FILE)
    file_['identifier'] = "pid:urn:wf_test_1a_ida_missing_file_format"
    file_['file_characteristics'] = {
        "file_created": "2014-01-17T08:19:31Z"
    }
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier?"
                      "dataset_format=datacite",
                      text=lxml.etree.tostring(BASE_DATACITE))
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:testimage/xml?"
                      "namespace=http://www.loc.gov/mix/v20",
                      text="<mix:mix\n")

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

    dataset = copy.deepcopy(BASE_DATASET)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    requests_mock.post("https://metaksi/rpc/datasets/set_preservation_"
                       "identifier?identifier=dataset_identifier")
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=[])
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

    :returns: ``None``
    """
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json={
            "identifier": "pid:urn:dir:wf1"
            }
    )
    dataset = copy.deepcopy(BASE_DATASET)
    add_files_to_dataset(["file_id"], dataset)
    requests_mock.get("https://metaksi/rest/v1/datasets/dataset_identifier",
                      json=dataset)
    requests_mock.get("https://metaksi/rest/v1/contracts/contract_identifier",
                      json=BASE_CONTRACT)
    adapter = requests_mock.patch(
        "https://metaksi/rest/v1/datasets/dataset_identifier"
    )
    file_ = copy.deepcopy(TXT_FILE)
    file_['identifier'] = "file_id"
    files = [file_]
    requests_mock.get(
        "https://metaksi/rest/v1/datasets/dataset_identifier/files",
        json=files
    )
    # Try to validate invalid dataset
    with pytest.raises(InvalidMetadataError) as exception_info:
        validate_metadata('dataset_identifier',
                          tests.conftest.UNIT_TEST_CONFIG_FILE,
                          set_preservation_state=True)

    # Check exception message
    assert str(exception_info.value).startswith(
        "Validation error in metadata of pid:urn:dir:wf1: 'directory_path' is"
        " a required property")
    _assert_metadata_validation_failed(
        adapter.last_request.json(),
        "Metadata did not pass validation: Validation error in metadata of "
        "pid:urn:dir:wf1: 'directory_path' is a required property\n\nFailed "
        "validating 'required' in schema:\n    {'properties': {'file_path': {'"
    )


def _assert_metadata_validation_passed(body_as_json):
    assert body_as_json == {
        'preservation_description': 'Metadata passed validation',
        'preservation_state': 70
    }


def _assert_metadata_validation_failed(body_as_json, description):
    assert body_as_json['preservation_state'] == 40
    assert body_as_json['preservation_description'].startswith(description)
