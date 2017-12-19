"""Tests for ``siptools_research.utils.validate_metadata`` module"""
import pytest
from siptools_research.utils.validate_metadata \
    import validate_dataset_metadata
from siptools_research.utils.validate_metadata \
    import validate_file_metadata
from jsonschema import ValidationError


def test_validate_dataset_metadata():
    """Test ``validate_dataset_metadata`` function"""
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

    assert validate_dataset_metadata(valid_dataset_metadata)
    with pytest.raises(ValidationError):
        assert not validate_dataset_metadata(invalid_dataset_metadata)


def test_validate_file_metadata():
    """Test ``validate_file_metadata`` function"""
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

    assert validate_file_metadata(valid_file_metadata)
    with pytest.raises(ValidationError):
        assert not validate_file_metadata(invalid_file_metadata)
