"""Tests for ``siptools_research.utils.validate_metadata`` module"""
import pytest
import jsonschema
import siptools_research.utils.validate_metadata


# pylint: disable=invalid-name
def test_validate_valid_dataset_metadata():
    """Test validation of valid dataset metadata"""
    valid_dataset_metadata = \
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
                        "type": {
                            "pref_label": {
                                "en": "label1"
                            }
                        }
                    }
                ]
            }
        }

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        valid_dataset_metadata,
        siptools_research.utils.validate_metadata.DATASET_METADATA_SCHEMA
    ) is None


def test_validate_invalid_dataset_metadata():
    """Test validation of invalid dataset metadata"""
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
        assert not jsonschema.validate(
            invalid_dataset_metadata,
            siptools_research.utils.validate_metadata.DATASET_METADATA_SCHEMA
        )


def test_validate_valid_file_metadata():
    """Test validation of valid file metadata."""
    valid_file_metadata = \
        {
            "checksum": {
                "algorithm": "sha2",
                "value": "habeebit"
            },
            "file_format": "html/text",
            "file_characteristics": {
                "file_created": "2014-01-17T08:19:31Z",
            }
        }

    # Validation of valid dataset should return 'None'
    assert jsonschema.validate(
        valid_file_metadata,
        siptools_research.utils.validate_metadata.FILE_METADATA_SCHEMA
    ) is None


def test_validate_invalid_file_metadata():
    """Test validation of invalid file metadata."""
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
        assert not jsonschema.validate(
            invalid_file_metadata,
            siptools_research.utils.validate_metadata.FILE_METADATA_SCHEMA
        )
