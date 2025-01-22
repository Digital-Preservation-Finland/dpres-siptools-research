"""Tests for :mod:`siptools_research.metadata_validator` module."""
import copy

import lxml.etree
import pytest
from requests.exceptions import HTTPError

import siptools_research
import tests.utils
from siptools_research.exceptions import (
    InvalidDatasetMetadataError,
    InvalidFileMetadataError,
)
from siptools_research.metadata_validator import validate_metadata
from siptools_research.metax import get_metax_client
from tests.metax_data.datasets import (
    BASE_DATACITE,
    BASE_DATASET,
    BASE_PROVENANCE,
    QVAIN_PROVENANCE,
)
from tests.metax_data.datasetsV3 import BASE_DATASET as BASE_DATASETV3
from tests.metax_data.files import (
    AUDIO_FILE,
    BASE_FILE,
    CSV_FILE,
    MKV_FILE,
    PDF_FILE,
    TIFF_FILE,
    TXT_FILE,
    VIDEO_FILE,
)


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


@pytest.mark.parametrize(
    'file_metadata',
    [TXT_FILE, CSV_FILE, TIFF_FILE, MKV_FILE, PDF_FILE, AUDIO_FILE, VIDEO_FILE]
)
def test_validate_metadata(config, requests_mock, file_metadata):
    """Test validation of dataset metadata that contains one file.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param file_metadata: Metadata of file included in dataset
    """
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file_metadata])

    assert validate_metadata('dataset_identifier', config)


@pytest.mark.parametrize(
    'provenance',
    [
        [],
        [BASE_PROVENANCE],
        [BASE_PROVENANCE, BASE_PROVENANCE],
        [QVAIN_PROVENANCE]
    ]
)
def test_validate_metadata_with_provenance(config, requests_mock, provenance):
    """Test validation of dataset metadata with provenance events.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param provenance: List of provenance events in dataset metadata
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["research_dataset"]["provenance"] = provenance
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )

    assert validate_metadata('dataset_identifier', config)


@pytest.mark.parametrize(
    'provenance',
    [
        [{}],
        [{"preservation_event": {}}]
    ]
)
def test_validate_metadata_with_invalid_provenance(config, requests_mock,
                                                   provenance):
    """Test validation of dataset metadata with invalid provenance events.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param provenance: List of provenance events in dataset metadata
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["research_dataset"]["provenance"] = provenance
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )

    expected_error = "None is not of type 'object'"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_metadata_multiple_files(config, requests_mock):
    """Test validation of dataset metadata that contains multiple files.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = "pid:urn:1"
    files[1]['identifier'] = "pid:urn:2"
    tests.utils.add_metax_v2_dataset(requests_mock, files=files)

    assert validate_metadata('dataset_identifier', config)


def test_validate_metadata_missing_file(config, requests_mock):
    """Test validate_metadata with an empty dataset.

    Function should raise InvalidDatasetMetadataError for datasets,
    which do not contain any files.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_v2_dataset(requests_mock)
    expected_error = "Dataset must contain at least one file"

    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_metadata_invalid(config, requests_mock):
    """Test validate_metadata.

    Function should raise exception with correct error message for
    invalid dataset.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get("/rest/v2/datasets/dataset_identifier",
                      json=dataset)

    # Try to validate invalid dataset
    expected_error = "None is not of type 'string'"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


@pytest.mark.parametrize(
    ('file_characteristics',
     'version_info'),
    [
        (
            {
                'file_format': 'application/unsupported',
                'format_version': '1.0'
            },
            ", version 1.0"),
        (
            {
                'file_format': 'application/unsupported'
            },
            "",
        )
    ]
)
def test_validate_invalid_file_type(config, file_characteristics, version_info,
                                    requests_mock):
    """Test validate_metadata.

    Function should raise exception with correct error message for
    unsupported file type.

    :param config: Configuration file
    :param file_characteristics: file characteristics dict in file
                                 metadata
    :param version_info: expected version information in exception
                         message
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    unsupported_file = copy.deepcopy(BASE_FILE)
    unsupported_file['file_characteristics'] = file_characteristics
    tests.utils.add_metax_v2_dataset(requests_mock, files=[unsupported_file])

    # Try to validate dataset with a file that has an unsupported
    # file_format
    expected_error = (
        "Validation error in file /path/to/file: Incorrect file format: "
        f"application/unsupported{version_info}"
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_metadata_invalid_file_path(config, requests_mock):
    """Test validate_metadata.

    Function should raise exception if some of the
    file paths point outside SIP.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    invalid_file = copy.deepcopy(TXT_FILE)
    invalid_file['file_path'] = "../../file_in_invalid_path"
    tests.utils.add_metax_v2_dataset(requests_mock, files=[invalid_file])

    # Try to validate invalid dataset
    expected_error = ("The file path of file pid:urn:identifier is invalid: "
                      "../../file_in_invalid_path")
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_file_metadata(config, requests_mock):
    """Test _validate_file_metadata.

    Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory in
    dataset.directories property.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    dataset = copy.deepcopy(BASE_DATASETV3)
    dataset['directories'] = [{'identifier': 'root_dir'}]
    file_1 = copy.deepcopy(TXT_FILE)
    file_1['identifier'] = 'file_identifier1'
    file_2 = copy.deepcopy(TXT_FILE)
    file_2['identifier'] = 'file_identifier2'

    files_adapter = requests_mock.get(
        "/rest/v2/datasets/dataset_identifier/files",
        json=[file_1, file_2],
        status_code=200
    )

    # This is here only for the support of V2 and the normalization
    requests_mock.get(
        "/rest/v2/datasets/dataset_identifier",
        json={
            "research_dataset": {
                "files": [
                    {"identifier": "file_identifier1"},
                    {"identifier": "file_identifier2"}
                ]
            }
        }
    )

    # Init metax client
    client = get_metax_client(config)

    # pylint: disable=protected-access
    siptools_research.metadata_validator._validate_file_metadata(dataset,
                                                                 client)

    assert files_adapter.call_count == 1


def test_validate_file_metadata_invalid_metadata(config, requests_mock):
    """Test ``_validate_file_metadata``.

    Function should raise exceptions with descriptive error messages.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata['file_characteristics'] = {
        "title": "A Great File"
    }
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file_metadata])

    # Init metax client
    client = get_metax_client(config)

    expected_error = (
        "Validation error in file /path/to/file: Incorrect file format: None"
    )
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        # pylint: disable=protected-access
        siptools_research.metadata_validator._validate_file_metadata(
            {
                'id': 'dataset_identifier'
            },
            client,
        )


def test_validate_metadata_http_error_raised(config, requests_mock):
    """Test validate_metadata.

    Function should raise HTTPError if Metax fails.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    tests.utils.add_metax_v2_dataset(requests_mock)
    requests_mock.get(
        "/rest/v2/datasets/dataset_identifier/files",
        status_code=500,
        reason="Something not to be shown to user"
    )

    expected_error = '500 Server Error: Something not to be shown to user'
    with pytest.raises(HTTPError, match=expected_error):
        validate_metadata('dataset_identifier', config)
