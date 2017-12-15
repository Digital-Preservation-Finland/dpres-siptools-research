"""Tests for ``siptools_research.utils.validate_metadata`` module"""
import pytest
from siptools_research.utils.validate_metadata \
    import validate_dataset_metadata
from jsonschema import ValidationError


def test_validate_dataset_metadata():
    """Test ``validate_dataset_metadata`` function"""
    valid_dataset = \
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

    invalid_dataset = \
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

    assert validate_dataset_metadata(valid_dataset)
    with pytest.raises(ValidationError):
        assert not validate_dataset_metadata(invalid_dataset)
