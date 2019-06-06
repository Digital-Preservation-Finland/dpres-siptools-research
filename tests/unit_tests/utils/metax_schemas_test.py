"""Tests for :mod:`siptools_research.utils.metax_schemas` module"""
import copy

import pytest
import jsonschema
import siptools_research.utils.metax_schemas as metax_schemas

VALID_DATASET_METADATA = {
    "preservation_identifier": "doi:test",
    "contract": {
        "identifier": "1"
    },
    "research_dataset": {
        "provenance": [
            {
                "preservation_event": {
                    "pref_label": {
                        "en": "ProvenanceText",
                    }
                },
                "description": {
                    "en": "en_description"
                },
                'event_outcome': {
                    "pref_label": {
                        "en": "outcome"
                    }
                },
                'outcome_description': {
                    "en": "outcome_description"
                }
            }
        ],
        "files": [
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
        ],
        "directories": [
        ]
    }
}


# pylint: disable=invalid-name
def test_validate_valid_dataset_metadata():
    """Test validation of valid dataset metadata with provenance. Defines a
    sample metadata dictionary that is known to be valid. The dictionary is
    then validated against ``DATASET_METADA_SCHEMA``.

    :returns: ``None``
    """

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(VALID_DATASET_METADATA,
                               metax_schemas.DATASET_METADATA_SCHEMA) is None


def test_validate_dataset_metadata_without_provenance():
    """Test validation of valid dataset metadata without provenance. Defines a
    sample metadata dictionary that has empty list of provenaces. The
    dictionary is then validated against ``DATASET_METADA_SCHEMA``.

    :returns: ``None``
    """
    invalid_dataset_metadata = copy.deepcopy(VALID_DATASET_METADATA)
    invalid_dataset_metadata['research_dataset']['provenance'] = []

    # Validation of valid dataset should return 'None'
    with pytest.raises(jsonschema.ValidationError) as error:
        assert not jsonschema.validate(invalid_dataset_metadata,
                                       metax_schemas.DATASET_METADATA_SCHEMA)

    assert error.value.message == '[] is too short'


def test_validate_invalid_dataset_metadata():
    """Test validation of invalid dataset metadata. The validation should raise
    ``ValidationError``.

    :returns: ``None``
    """
    # Create invalid metadata by deleting required key from valid dataset
    invalid_dataset_metadata = copy.deepcopy(VALID_DATASET_METADATA)
    del invalid_dataset_metadata["preservation_identifier"]

    # Validation of invalid dataset should raise error
    with pytest.raises(jsonschema.ValidationError) as error:
        assert not jsonschema.validate(invalid_dataset_metadata,
                                       metax_schemas.DATASET_METADATA_SCHEMA)

    assert error.value.message == ("'preservation_identifier' is a "
                                   "required property")


def test_validate_valid_file_metadata():
    """Test validation of valid file metadata. Defines a sample metadata
    dictionary that is known to be valid. The dictionary is then validated
    against ``FILE_METADA_SCHEMA``.

    :returns: ``None``
    """
    valid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
                "value": "habeebit"
            },
            "file_path": "path/to/file",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            },
            "parent_directory": {
                "identifier": "pid:urn:dir:1",
            },
            "file_characteristics": {
                "file_created": "2014-01-17T08:19:31Z",
                "file_format": "html/text"
            }
        }

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(valid_file_metadata,
                               metax_schemas.FILE_METADATA_SCHEMA) is None


def test_validate_valid_file_metadata_optional_attribute_missing():
    """Test validation of valid file metadata. Defines a sample metadata
    dictionary that is known to be valid. The dictionary is then validated
    against ``FILE_METADA_SCHEMA``.

    :returns: ``None``
    """
    valid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
                "value": "habeebit"
            },
            "file_path": "path/to/file",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            },
            "parent_directory": {
                "identifier": "pid:urn:dir:1",
            },
            "file_characteristics": {
                "file_format": "html/text"
            }
        }

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(valid_file_metadata,
                               metax_schemas.FILE_METADATA_SCHEMA) is None


def test_validate_invalid_file_metadata():
    """Test validation of invalid file metadata. The validation should raise
    ``ValidationError``.


    :returns: ``None``
    """
    invalid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
            },
            "file_format": "html/text",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            },
            "file_characteristics": {
                "file_created": "2014-01-17T08:19:31Z",
            }
        }

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(invalid_file_metadata,
                                       metax_schemas.FILE_METADATA_SCHEMA)

    assert excinfo.value.message == "'file_path' is a required property"


def test_validate_invalid_file_charset():
    """Test validation of file metadata that contains invalid file encoding.
    The validation should raise ``ValidationError``.


    :returns: ``None``
    """
    invalid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
                "value": "habeebit"
            },
            "file_path": "path/to/file",
            "file_storage": {
                "identifier": "urn:nbn:fi:att:file-storage-ida"
            },
            "parent_directory": {
                "identifier": "pid:urn:dir:1",
            },
            "file_characteristics": {
                "file_created": "2014-01-17T08:19:31Z",
                "file_format": "html/text",
                "file_encoding": "foo"
            }
        }

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError) as excinfo:
        assert not jsonschema.validate(invalid_file_metadata,
                                       metax_schemas.FILE_METADATA_SCHEMA)

    assert excinfo.value.message \
        == "'foo' is not one of ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']"


def test_validate_valid_contract():
    """Test validation of valid contract metadata


    :returns: ``None``
    """
    valid_contract_metadata = \
        {
            "contract_json": {
                "identifier": "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
                "organization": {
                    "name": "Testiorganisaatio"
                }
            }
        }

    jsonschema.validate(valid_contract_metadata,
                        metax_schemas.CONTRACT_METADATA_SCHEMA)


def test_validate_invalid_contract():
    """Test validation of invalid contract metadata (name is not string)


    :returns: ``None``
    """
    invalid_contract_metadata = \
        {
            "contract_json": {
                "identifier": "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
                "organization": {
                    "name": 1234
                }
            }
        }

    with pytest.raises(jsonschema.ValidationError) as excinfo:
        jsonschema.validate(invalid_contract_metadata,
                            metax_schemas.CONTRACT_METADATA_SCHEMA)

    assert excinfo.value.message == "1234 is not of type 'string'"


def test_validate_dataset_with_directories():
    """Test validation of valid dataset metadata that contains only directories
    and no files. Defines a sample metadata dictionary that is known to be
    valid. The dictionary is then validated against ``DATASET_METADA_SCHEMA``.

    :returns: ``None``
    """
    valid_dataset_metadata = copy.deepcopy(VALID_DATASET_METADATA)
    del valid_dataset_metadata['research_dataset']['files']

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(valid_dataset_metadata,
                               metax_schemas.DATASET_METADATA_SCHEMA) is None


def test_validate_dataset_no_files_and_directories():
    """Test validation of dataset metadata without directories nor files
    attribute present in dataset. Defines a sample metadata dictionary that is
    known to be valid. The dictionary is then validated against
    ``DATASET_METADA_SCHEMA``.

    :returns: ``None``
    """
    invalid_dataset_metadata = copy.deepcopy(VALID_DATASET_METADATA)
    del invalid_dataset_metadata['research_dataset']['files']
    del invalid_dataset_metadata['research_dataset']['directories']

    with pytest.raises(jsonschema.ValidationError) as excinfo:
        jsonschema.validate(invalid_dataset_metadata,
                            metax_schemas.DATASET_METADATA_SCHEMA)

    assert excinfo.value.message.endswith(
        "is not valid under any of the given schemas"
    )
