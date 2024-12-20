"""Tests for :mod:`siptools_research.schemas` module."""
import copy

import pytest
import jsonschema
import siptools_research.schemas

from tests.metax_data.datasetsV3 import (
    BASE_DATASET, BASE_PROVENANCE, QVAIN_PROVENANCE
)
from tests.metax_data.filesV3 import (
    TXT_FILE, CSV_FILE, PDF_FILE, TIFF_FILE, AUDIO_FILE, MKV_FILE, VIDEO_FILE
)
from tests.metax_data.contractsV3 import BASE_CONTRACT

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


def test_validate_invalid_dataset_metadata():
    """Test validation of invalid dataset metadata.

    The validation should raise ``ValidationError``.

    :returns: ``None``
    """
    # Create invalid metadata by deleting required key from valid
    # dataset
    invalid_dataset_metadata = copy.deepcopy(BASE_DATASET)
    del invalid_dataset_metadata["preservation"]["id"]

    # Validation of invalid dataset should raise error
    with pytest.raises(jsonschema.ValidationError) as error:
        assert not jsonschema.validate(
            invalid_dataset_metadata,
            siptools_research.schemas.DATASET_METADATA_SCHEMA
        )

    assert error.value.message == (
        "'id' is a required property"
    )


@pytest.mark.parametrize(
    'file_metadata',
    (TXT_FILE, CSV_FILE, PDF_FILE, TIFF_FILE, AUDIO_FILE, MKV_FILE, VIDEO_FILE)
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
    del invalid_file_metadata['pathname']

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.schemas.FILE_METADATA_SCHEMA
        )

    assert excinfo.value.message == "'pathname' is a required property"


def test_validate_invalid_file_charset():
    """Test validation of invalid file encoding.

    Validate file metadata with file encoding that is not supported. The
    validation should raise ``ValidationError``.

    :returns: ``None``
    """
    invalid_file_metadata = copy.deepcopy(TXT_FILE)
    invalid_file_metadata['characteristics']['encoding'] = "foo"

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.schemas.FILE_METADATA_SCHEMA
        )

    assert excinfo.value.message == \
        "'foo' is not one of ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']"


def test_validate_invalid_checksum_algorithm():
    """Test validation of invalid checksum algorithm.

    The validation should raise ``ValidationError``.
    """
    invalid_file_metadata = copy.deepcopy(TXT_FILE)
    invalid_file_metadata['checksum'] = "sha2:123"

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.schemas.FILE_METADATA_SCHEMA
        )

    assert excinfo.value.message == (
        "'sha2:123' does not match '^(md5|sha256|sha512|sha1|sha224|sha383):[a-z0-9_]+$'")


@pytest.mark.parametrize(
    'attribute',
    (
        'csv_delimiter',
        'csv_has_header',
        'csv_record_separator',
        'csv_quoting_char'
    )
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


def test_validate_valid_contract():
    """Test validation of valid contract metadata.

    :returns: ``None``
    """
    jsonschema.validate(BASE_CONTRACT,
                        siptools_research.schemas.CONTRACT_METADATA_SCHEMA)


def test_validate_invalid_contract():
    """Test validation of invalid contract metadata.

    Validate contract metadata with wrong type of organization name
    (integer instead of string). Validation should raise error.

    :returns: ``None``
    """
    invalid_contract_metadata = copy.deepcopy(BASE_CONTRACT)
    invalid_contract_metadata['organization']['name'] = 1234

    with pytest.raises(jsonschema.ValidationError) as excinfo:
        jsonschema.validate(invalid_contract_metadata,
                            siptools_research.schemas.CONTRACT_METADATA_SCHEMA)

    assert excinfo.value.message == "1234 is not of type 'string'"


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
