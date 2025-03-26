"""Tests for :mod:`siptools_research.metadata_validator` module."""
import copy

import lxml.etree
import pytest
from file_scraper.defaults import (ACCEPTABLE, BIT_LEVEL,
                                   BIT_LEVEL_WITH_RECOMMENDED, RECOMMENDED,
                                   UNACCEPTABLE)
from requests.exceptions import HTTPError

import siptools_research
import tests.utils
from siptools_research.exceptions import (
    InvalidDatasetFileError,
    InvalidDatasetMetadataError,
    InvalidFileMetadataError
)
from siptools_research.metadata_validator import validate_metadata
from siptools_research.metax import get_metax_client
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import (
    AUDIO_FILE,
    CSV_FILE,
    MKV_FILE,
    PDF_FILE,
    SEG_Y_FILE,
    TIFF_FILE,
    TXT_FILE,
    VIDEO_FILE
)


@pytest.mark.parametrize(
    "file_metadata",
    [
        TXT_FILE,
        CSV_FILE,
        TIFF_FILE,
        MKV_FILE,
        PDF_FILE,
        AUDIO_FILE,
        VIDEO_FILE,
    ]
)
def test_validate_metadata(config, requests_mock, file_metadata):
    """Test validation of dataset metadata that contains one file.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param file_metadata: Metadata of file included in dataset in
        Metax
    """
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    assert validate_metadata('dataset_identifier', config)


def test_validate_metadata_multiple_files(config, requests_mock):
    """Test validation of dataset metadata that contains multiple files.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax
    files = [copy.deepcopy(TXT_FILE), copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = "pid:urn:1"
    files[1]['identifier'] = "pid:urn:2"
    tests.utils.add_metax_dataset(requests_mock, files=files)

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
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["preservation"]["contract"] = None
    requests_mock.get("/v3/datasets/dataset_identifier", json=dataset)

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
    # Mock Metax
    invalid_file = copy.deepcopy(TXT_FILE)
    invalid_file["pathname"] = "../../file_in_invalid_path"
    tests.utils.add_metax_dataset(requests_mock, files=[invalid_file])

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
    dataset = copy.deepcopy(BASE_DATASET)

    # Mock Metax
    file_1 = copy.deepcopy(TXT_FILE)
    file_1['id'] = 'file_identifier1'
    file_2 = copy.deepcopy(TXT_FILE)
    file_2['id'] = 'file_identifier2'
    files_adapter = requests_mock.get(
        "/v3/datasets/dataset_identifier/files",
        json={"next": None, "results": [file_1, file_2]},
        status_code=200
    )

    # Init metax client
    client = get_metax_client(config)

    # pylint: disable=protected-access
    siptools_research.metadata_validator._validate_file_metadata(dataset,
                                                                 client)

    assert files_adapter.call_count == 1


@pytest.mark.usefixtures('testmongoclient')
@pytest.mark.parametrize(
    "files,expected_error,expected_error_file_ids",
    [
        (
            [TIFF_FILE | {"non_pas_compatible_file": SEG_Y_FILE["id"]}],
            "Dataset contains DPRES compatible files without bit-level "
            "counterparts.",
            [TIFF_FILE["id"]]
        ),
        (
            [SEG_Y_FILE | {"pas_compatible_file": TIFF_FILE["id"]}],
            "Dataset contains bit-level files without DPRES compatible "
            "counterparts.",
            [SEG_Y_FILE["id"]],
        ),
        (
            [
                SEG_Y_FILE | {"pas_compatible_file": TIFF_FILE["id"]},
                TXT_FILE | {"non_pas_compatible_file": CSV_FILE["id"]}
            ],
            "Dataset contains both bit-level and DPRES compatible files "
            "without DPRES compatible / bit-level counterparts.",
            [
                SEG_Y_FILE["id"], TXT_FILE["id"]
            ]
        )
    ]
)
def test_detect_missing_file_link(
        config, requests_mock, files,
        expected_error, expected_error_file_ids):
    """
    Test creating a METS for a dataset with one file that is marked as
    DPRES compatible but is missing its bit-level counterpart.

    Ensure an exception is raised.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)

    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=files
    )

    # Init and run task
    with pytest.raises(InvalidDatasetFileError) as exc:
        siptools_research.metadata_validator.validate_metadata(
            dataset_id=dataset["id"],
            config=config
        )

    assert expected_error in str(exc.value)
    assert expected_error_file_ids == exc.value.files


@pytest.mark.parametrize(
    "is_linked_bitlevel", [True, False]
)
def test_validate_file_metadata_missing_file_format(
        config, requests_mock, is_linked_bitlevel):
    """
    Test that file format version is required unless the file is a bit-level
    file linked to a DPRES compatible file
    """
    dataset = copy.deepcopy(BASE_DATASET)

    file = copy.deepcopy(TXT_FILE)
    file["characteristics"]["file_format_version"] = None

    file_b = copy.deepcopy(CSV_FILE)

    if is_linked_bitlevel:
        file["pas_compatible_file"] = file_b["id"]
        file_b["non_pas_compatible_file"] = file["id"]

    requests_mock.get(
        "/v3/datasets/dataset_identifier/files",
        json={"next": None, "results": [file, file_b]}
    )

    # Init metax client
    metax = get_metax_client(config)

    if not is_linked_bitlevel:
        with pytest.raises(InvalidFileMetadataError) as exc:
            siptools_research.metadata_validator._validate_file_metadata(
                dataset=dataset,
                metax_client=metax
            )

        assert "Non bit-level file must have `file_format_version` set" \
            in str(exc.value)
    else:
        siptools_research.metadata_validator._validate_file_metadata(
            dataset=dataset,
            metax_client=metax
        )


@pytest.mark.parametrize(
    "file,grade,expected_error",
    [
        (
            # Recommended is fine by itself
            {}, RECOMMENDED, None
        ),
        (
            # Acceptable is acceptable
            {}, ACCEPTABLE, None
        ),
        (
            # Bit-level file is fine as itself
            {}, BIT_LEVEL, None
        ),
        (
            # Bit-level with recommended requires a PAS compatible file
            {}, BIT_LEVEL_WITH_RECOMMENDED,
            "is not linked to a PAS compatible file"
        ),
        (
            {"pas_compatible_file": "pas-compatible-file-id"},
            BIT_LEVEL_WITH_RECOMMENDED, None
        ),
        (
            {}, UNACCEPTABLE, "is not linked to a PAS compatible file"
        ),
        (
            {"pas_compatible_file": "pas-compatible-file-id"},
            UNACCEPTABLE, None
        ),
        (
            # Acceptable file cannot present itself as PAS compatible
            {"non_pas_compatible_file": "non-pas-compatible-file-id"},
            ACCEPTABLE,
            "does not have the required 'fi-dpres-recommended-file-format' "
            "grade"
        ),
        (
            # Unacceptable file is not considered PAS compatible despite
            # possible link
            {"non_pas_compatible_file": "non-pas-compatible-file-id"},
            UNACCEPTABLE,
            "is not linked to a PAS compatible file"
        )
    ]
)
def test_validate_file_grades_and_links(
        config, requests_mock, file, grade, expected_error):
    """
    Validate files with different PAS compatible file links and grades
    and ensure wrong combinations fail the validation
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)

    files = []

    file = copy.deepcopy(TXT_FILE) | file
    file["characteristics_extension"]["grade"] = grade

    files.append(file)

    # Also create the PAS/non-PAS compatible counterpart if it was defined
    if file.get("pas_compatible_file"):
        pas_file = TXT_FILE | {
            "id": file["pas_compatible_file"],
            "non_pas_compatible_file": file["id"]
        }
        files.append(pas_file)
    if file.get("non_pas_compatible_file"):
        non_pas_file = TXT_FILE | {
            "id": file["non_pas_compatible_file"],
            "pas_compatible_file": file["id"]
        }
        files.append(non_pas_file)

    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=files
    )

    # Init and run task
    if expected_error:
        with pytest.raises(InvalidDatasetFileError) as exc:
            siptools_research.metadata_validator.validate_metadata(
                dataset_id=dataset["id"],
                config=config
            )

        assert expected_error in str(exc.value)
    else:
        siptools_research.metadata_validator.validate_metadata(
            dataset_id=dataset["id"],
            config=config
        )


def test_validate_metadata_http_error_raised(config, requests_mock):
    """Test validate_metadata.

    Function should raise HTTPError if Metax fails.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)
    requests_mock.get(
        "/v3/datasets/dataset_identifier/files",
        status_code=500,
        reason="Something not to be shown to user"
    )

    expected_error = '500 Server Error: Something not to be shown to user'
    with pytest.raises(HTTPError, match=expected_error):
        validate_metadata('dataset_identifier', config)
