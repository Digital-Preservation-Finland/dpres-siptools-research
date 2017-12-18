"""Validation of JSON metadata retrieved from Metax"""
from jsonschema import validate

# JSON schema for dataset metadata
# http://json-schema.org/
DATASET_SCHEMA = \
    {
        "type": "object",
        "required": ["research_dataset"], # JSON must have "research_dataset"
        "properties": {
            "research_dataset": {
                "type": "object", # "research_dataset" is dict
                "required": ["files"], # "research_dataset" must have "files"
                "properties": { # "research_dataset" has attributes
                    "files": { # "research_dataset" has attribute "files"
                        "type": "array", # files is list
                        "items": {
                            "type": "object",
                            "required": ["identifier", "type"],
                            "properties": {
                                "identifier": {
                                    "type": "string"
                                },
                                "type": {
                                    "type": "object",
                                    "required": ["pref_label"],
                                    "properties": {
                                        "pref_label": {
                                            "type": "object",
                                            "required": ["en"],
                                            "properties": {
                                                "en": {
                                                    "type":"string"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

def validate_dataset_metadata(metadata):
    """Validate dataset metadata retrieved from Metax. Returns ``True`` if
    dataset has all attributes required by tasks of packaging workflow.

    :metadata: Dataset metadata from metax (dict)
    :returns: ``True`` or ``False``
    """

    return bool(validate(metadata, DATASET_SCHEMA) is None)
