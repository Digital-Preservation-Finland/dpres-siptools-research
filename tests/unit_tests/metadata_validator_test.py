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
import tests.metax_data.filesV3
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
    ("file_metadata", "v2_file_metadata"),
    [
        (
            tests.metax_data.filesV3.TXT_FILE,
            TXT_FILE,
        ),
        (
            tests.metax_data.filesV3.CSV_FILE,
            CSV_FILE,
        ),
        (
            tests.metax_data.filesV3.TIFF_FILE,
            TIFF_FILE,
        ),
        (
            tests.metax_data.filesV3.MKV_FILE,
            MKV_FILE,
        ),
        (
            tests.metax_data.filesV3.PDF_FILE,
            PDF_FILE,
        ),
        (
            tests.metax_data.filesV3.AUDIO_FILE,
            AUDIO_FILE,
        ),
        (
            tests.metax_data.filesV3.VIDEO_FILE,
            VIDEO_FILE
        ),
    ]
)
def test_validate_metadata(config, requests_mock, file_metadata,
                           v2_file_metadata):
    """Test validation of dataset metadata that contains one file.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param file_metadata: Metadata of file included in dataset in
        Metax V3
    :param v2_file_metadata: Metadata of file included in dataset in
        Metax V2
    """
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    tests.utils.add_metax_v2_dataset(requests_mock, files=[v2_file_metadata])

    assert validate_metadata('dataset_identifier', config)


@pytest.mark.parametrize(
    ("provenance", "v2_provenance"),
    [
        # No provenance
        (
            [],
            [],
        ),
        # One life cycle event
        (
            [tests.metax_data.datasetsV3.QVAIN_PROVENANCE],
            [QVAIN_PROVENANCE]
        ),
        # One preservation event
        (
            [tests.metax_data.datasetsV3.BASE_PROVENANCE],
            [BASE_PROVENANCE],
        ),
        # Two events of same type
        (
            [
                tests.metax_data.datasetsV3.BASE_PROVENANCE,
                tests.metax_data.datasetsV3.BASE_PROVENANCE
            ],
            [
                BASE_PROVENANCE,
                BASE_PROVENANCE
            ],
        ),
    ]
)
def test_validate_metadata_with_provenance(config, requests_mock, provenance,
                                           v2_provenance):
    """Test validation of dataset metadata with provenance events.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param provenance: List of provenance events in dataset metadata
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["provenance"] = provenance
    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=[tests.metax_data.filesV3.TXT_FILE]
    )

    # Mock Metax API V2
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["research_dataset"]["provenance"] = v2_provenance
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )

    assert validate_metadata('dataset_identifier', config)


def test_validate_metadata_multiple_files(config, requests_mock):
    """Test validation of dataset metadata that contains multiple files.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax API V3
    files = [copy.deepcopy(tests.metax_data.filesV3.TXT_FILE),
             copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)]
    files[0]['identifier'] = "pid:urn:1"
    files[1]['identifier'] = "pid:urn:2"
    tests.utils.add_metax_dataset(requests_mock, files=files)

    # Mock Metax API V2
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
    tests.utils.add_metax_dataset(requests_mock)
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
    # Mock metax API V3
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["preservation"]["contract"] = None
    requests_mock.get("/v3/datasets/dataset_identifier", json=dataset)

    # Mock metax API V2
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get("/rest/v2/datasets/dataset_identifier",
                      json=dataset)

    # Try to validate invalid dataset
    expected_error = "None is not of type 'string'"
    with pytest.raises(InvalidDatasetMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_metadata_invalid_file_path(config, requests_mock):
    """Test validate_metadata.

    Function should raise exception if some of the
    file paths point outside SIP.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax API V3
    invalid_file = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    invalid_file["pathname"] = "../../file_in_invalid_path"
    tests.utils.add_metax_dataset(requests_mock, files=[invalid_file])

    # Mock Metax API V2
    invalid_file = copy.deepcopy(TXT_FILE)
    invalid_file['file_path'] = "../../file_in_invalid_path"
    tests.utils.add_metax_v2_dataset(requests_mock, files=[invalid_file])

    # Try to validate invalid dataset
    expected_error = ("The file path of file pid:urn:identifier is invalid: "
                      "../../file_in_invalid_path")
    with pytest.raises(InvalidFileMetadataError, match=expected_error):
        validate_metadata('dataset_identifier', config)


def test_validate_file_metadata(config, requests_mock, request):
    """Test _validate_file_metadata.

    Check that dataset directory caching is working correctly in
    DatasetConsistency when the files have common root directory in
    dataset.directories property.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param request: Pytest CLI arguments
    """
    dataset = copy.deepcopy(BASE_DATASETV3)
    dataset['directories'] = [{'identifier': 'root_dir'}]

    # Mock Metax API V3
    file_1 = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    file_1['id'] = 'file_identifier1'
    file_2 = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    file_2['id'] = 'file_identifier2'
    files_adapter = requests_mock.get(
        "/v3/datasets/dataset_identifier/files",
        json={"next": None, "results": [file_1, file_2]},
        status_code=200
    )

    # Mock Metax API V2
    file_1 = copy.deepcopy(TXT_FILE)
    file_1['identifier'] = 'file_identifier1'
    file_2 = copy.deepcopy(TXT_FILE)
    file_2['identifier'] = 'file_identifier2'
    if not request.config.getoption('--v3'):
        # Overwrite Metax V3 files_adapter
        files_adapter = requests_mock.get(
            "/rest/v2/datasets/dataset_identifier/files",
            json=[file_1, file_2],
            status_code=200
        )
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


def test_validate_metadata_http_error_raised(config, requests_mock):
    """Test validate_metadata.

    Function should raise HTTPError if Metax fails.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax V3 API
    tests.utils.add_metax_dataset(requests_mock)
    requests_mock.get(
        "/v3/datasets/dataset_identifier/files",
        status_code=500,
        reason="Something not to be shown to user"
    )

    # Mock Metax V2 API
    tests.utils.add_metax_v2_dataset(requests_mock)
    requests_mock.get(
        "/rest/v2/datasets/dataset_identifier/files",
        status_code=500,
        reason="Something not to be shown to user"
    )

    expected_error = '500 Server Error: Something not to be shown to user'
    with pytest.raises(HTTPError, match=expected_error):
        validate_metadata('dataset_identifier', config)
