"""Tests for :mod:`siptools_research.schemas` module."""
import copy

import jsonschema
import pytest

import siptools_research.schemas
from tests.metax_data.datasets import (
    BASE_DATASET,
    BASE_PROVENANCE,
    QVAIN_PROVENANCE,
)
from tests.metax_data.files import (
    AUDIO_FILE,
    CSV_FILE,
    MKV_FILE,
    PDF_FILE,
    TIFF_FILE,
    TXT_FILE,
    VIDEO_FILE,
)

SAMPLE_FILES = [
    {
        "title": "File 1",
        "identifier": "pid1",
        "file_storage": {
            "identifier": "urn:nbn:fi:att:file-storage-ida"
        },
        "use_category": {
            "pref_label": {
                "en": "label1"
            }
        }
    },
    {
        "title": "File 2",
        "identifier": "pid2",
        "file_storage": {
            "identifier": "urn:nbn:fi:att:file-storage-ida"
        },
        "use_category": {
            "pref_label": {
                "en": "label1"
            }
        }
    }
]


@pytest.mark.parametrize(
    "provenance",
    [
        # One provenance events
        [BASE_PROVENANCE],
        # Multiple provenance events
        [BASE_PROVENANCE, BASE_PROVENANCE],
        # Empty list of provenance events
        [],
        # Provenance made in Qvain
        [QVAIN_PROVENANCE]
    ])
def test_validate_dataset_metadata_with_provenance(provenance):
    """Test validation of valid dataset with provenance metadata.

    :param provenance: Value of "provenance" key in metadata.
    :returns: ``None``
    """
    dataset_metadata = copy.deepcopy(BASE_DATASET)
    dataset_metadata['provenance'] = provenance

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        dataset_metadata,
        siptools_research.schemas.DATASET_METADATA_SCHEMA
    ) is None


@pytest.mark.parametrize(
    (
        "provenance",
        "error_message",
    ),
    [
        # Every event should contain at least "lifecycle_event" object
        (
            [{}],
            "'lifecycle_event' is a required property",
        ),
        (
            [{"lifecycle_event": {}}],
            "'pref_label' is a required property",
        ),
    ]
)
def test_validate_metadata_with_invalid_provenance(provenance, error_message):
    """Test validation of invalid provenance events.

    :param provenance: List of provenance events in dataset metadata
    :param error_message: Expected error message
    :returns: ``None``
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["provenance"] = provenance

    # Validation of invalid dataset should raise error
    with pytest.raises(jsonschema.ValidationError) as error:
        assert not jsonschema.validate(
            dataset,
            siptools_research.schemas.DATASET_METADATA_SCHEMA
        )

    assert error.value.message == error_message


def test_validate_invalid_dataset_metadata():
    """Test validation of invalid dataset metadata.

    The validation should raise ``ValidationError``.

    :returns: ``None``
    """
    # Create invalid metadata by deleting required key from valid
    # dataset
    invalid_dataset_metadata = copy.deepcopy(BASE_DATASET)
    del invalid_dataset_metadata["preservation"]["contract"]

    # Validation of invalid dataset should raise error
    with pytest.raises(jsonschema.ValidationError) as error:
        assert not jsonschema.validate(
            invalid_dataset_metadata,
            siptools_research.schemas.DATASET_METADATA_SCHEMA
        )

    assert error.value.message == (
        "'contract' is a required property"
    )


@pytest.mark.parametrize(
    'file_metadata',
    [TXT_FILE, CSV_FILE, PDF_FILE, TIFF_FILE, AUDIO_FILE, MKV_FILE, VIDEO_FILE]
)
def test_validate_valid_file_metadata(file_metadata):
    """Test validation of valid file metadata.

    Validate valid file metadata against ``FILE_METADATA_SCHEMA``.

    :param file_metadata: metadata dictionary to be validated
    :returns: ``None``
    """
    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        file_metadata,
        siptools_research.schemas.FILE_METADATA_SCHEMA
    ) is None


def test_validate_valid_file_metadata_optional_attribute_missing():
    """Test validation of valid file metadata.

    Create file metadata that does not have all optional attributes. The
    metadata is then validated against ``FILE_METADATA_SCHEMA``.

    :returns: ``None``
    """
    valid_file_metadata = copy.deepcopy(TXT_FILE)
    del valid_file_metadata['csc_project']

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        valid_file_metadata,
        siptools_research.schemas.FILE_METADATA_SCHEMA
    ) is None


def test_validate_invalid_file_metadata():
    """Test validation of invalid file metadata.

    Create file metadata that does not have all required attributes. The
    validation of the metadata should raise ``ValidationError``.

    :returns: ``None``
    """
    invalid_file_metadata = copy.deepcopy(TXT_FILE)
    del invalid_file_metadata['characteristics']

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.schemas.FILE_METADATA_SCHEMA
        )

    assert excinfo.value.message == "'characteristics' is a required property"


@pytest.mark.parametrize(
    'attribute',
    [
        'csv_delimiter',
        'csv_has_header',
        'csv_record_separator',
        'csv_quoting_char',
        'encoding'
    ]
)
def test_validate_invalid_csv(attribute):
    """Test validation of invalid CSV metadata.

    Create CSV file metadata that does not have all attributes that
    required for CSV files. The validation should raise
    ``ValidationError``.

    :param attribute: Attribute missing from metadata
    :returns: ``None``
    """
    invalid_file_metadata = copy.deepcopy(CSV_FILE)
    del invalid_file_metadata['characteristics'][attribute]

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.schemas.FILE_METADATA_SCHEMA
        )

    assert excinfo.value.message == \
        f"'{attribute}' is a required property"


def test_validate_dataset_with_files():
    """Test validation of dataset that has some files.

    Defines a valid sample metadata dictionary that contains some files.
    The dictionary is then validated against
    ``DATASET_METADATA_SCHEMA``.

    :returns: ``None``
    """
    valid_dataset_metadata = copy.deepcopy(BASE_DATASET)
    valid_dataset_metadata['files'] = SAMPLE_FILES

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        valid_dataset_metadata,
        siptools_research.schemas.DATASET_METADATA_SCHEMA
    ) is None


def test_validate_dataset_without_files():
    """Test validation of dataset without files.

    Defines a valid sample metadata dictionary that does not contain any
    files. The dictionary is then validated against
    ``DATASET_METADATA_SCHEMA``.

    :returns: ``None``
    """
    valid_dataset_metadata = copy.deepcopy(BASE_DATASET)
    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        valid_dataset_metadata,
        siptools_research.schemas.DATASET_METADATA_SCHEMA
    ) is None
