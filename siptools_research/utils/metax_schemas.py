"""Validation schemas for JSON metadata retrieved from Metax."""

# JSON schema for dataset metadata
# http://json-schema.org/
DATASET_METADATA_SCHEMA = \
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


# JSON schema for file metadata
FILE_METADATA_SCHEMA = \
    {
        "type": "object",
        "required": ["checksum", "file_characteristics", "file_format",
                     "file_path"],
        "properties": {
            "checksum": {
                "type": "object",
                "required": ["algorithm", "value"]
            },
            "file_characteristics": {
                "type": "object",
                "required": ["file_created"]
            }
        }
    }
