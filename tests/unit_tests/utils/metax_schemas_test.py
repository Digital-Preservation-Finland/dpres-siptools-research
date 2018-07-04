"""Tests for ``siptools_research.utils.metax_schemas`` module"""
import pytest
import jsonschema
import siptools_research.utils.metax_schemas as metax_schemas


# pylint: disable=invalid-name
def test_validate_valid_dataset_metadata():
    """Test validation of valid dataset metadata. Defines a sample metadata
    dictionary that is known to be valid. The dictionary is then validated
    against ``DATASET_METADA_SCHEMA``.

    :returns: None
    """
    valid_dataset_metadata = \
        {
            "contract": {
                "id": 1
            },
            "research_dataset": {
                "provenance": [
                    {
                        "preservation_event": {
                            "identifier": "identifierURL",
                            "pref_label": {
                                "en": "ProvenanceText",
                            }
                        },
                        "description": {
                            "en": "en_description"
                        },
                        "temporal": {
                            "start_date": "17.9.1991"
                        }
                    }
                ],
                "files": [
                    {
                        "title": "File 1",
                        "identifier": "pid1",
                        "use_category": {
                            "pref_label": {
                                "en": "label1"
                            }
                        }
                    },
                    {
                        "title": "File 2",
                        "identifier": "pid2",
                        "use_category": {
                            "pref_label": {
                                "en": "label1"
                            }
                        }
                    }
                ]
            }
        }

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(valid_dataset_metadata,
                               metax_schemas.DATASET_METADATA_SCHEMA) is None


def test_validate_invalid_dataset_metadata():
    """Test validation of invalid dataset metadata. The validation should raise
    ``ValidationError``.

    :returns: None
    """
    invalid_dataset_metadata = \
        {
            "research_dataset": {
                "files": [
                    {
                        "title": "File 1",
                        "identifier": "pid1",
                        "type": {
                            "pref_label": {
                                "en": "label1"
                            }
                        }
                    },
                    {
                        "title": "File 2",
                        "identifier": "pid2",
                    }
                ]
            }
        }

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError):
        assert not jsonschema.validate(invalid_dataset_metadata,
                                       metax_schemas.DATASET_METADATA_SCHEMA)


def test_validate_valid_file_metadata():
    """Test validation of valid file metadata. Defines a sample metadata
    dictionary that is known to be valid. The dictionary is then validated
    against ``FILE_METADA_SCHEMA``.

    :returns: None
    """
    valid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
                "value": "habeebit"
            },
            "file_path": "path/to/file",
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


def test_validate_invalid_file_metadata():
    """Test validation of invalid file metadata. The validation should raise
    ``ValidationError``.

    :returns: None
    """
    invalid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
            },
            "file_format": "html/text",
            "file_characteristics": {
                "file_created": "2014-01-17T08:19:31Z",
            }
        }

    # Validation of invalid dataset raise error
    with pytest.raises(jsonschema.ValidationError):
        assert not jsonschema.validate(invalid_file_metadata,
                                       metax_schemas.FILE_METADATA_SCHEMA)
