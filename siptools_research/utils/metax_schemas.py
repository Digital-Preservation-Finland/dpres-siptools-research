"""Validation schemas for JSON metadata retrieved from Metax."""

# JSON schema for dataset metadata
# http://json-schema.org/
DATASET_METADATA_SCHEMA = \
    {
        "type": "object",
        "required": ["research_dataset", "contract"],
        "properties": {
            "contract": {
                "type": "object",
                "required": ["id"]
            },
            "research_dataset": {
                "type": "object", # "research_dataset" is dict
                "required": ["files"], # "research_dataset" must have "files"
                "properties": { # "research_dataset" has attributes
                    "files": { # "research_dataset" has attribute "files"
                        "type": "array", # files is list
                        "items": {
                            "type": "object",
                            "required": ["identifier", "use_category"],
                            "properties": {
                                "identifier": {
                                    "type": "string"
                                },
                                "use_category": {
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


# JSON schema for file metadata
FILE_METADATA_SCHEMA = \
    {
        "type": "object",
        "required": ["checksum", "file_characteristics", "file_format",
                     "file_path", "parent_directory"],
        "properties": {
            "checksum": {
                "type": "object",
                "required": ["algorithm", "value"]
            },
            "file_characteristics": {
                "type": "object",
                "required": ["file_created"]
            },
            "parent_directory": {
                "type": "object",
                "required": ["identifier"]
            }
        }
    }
