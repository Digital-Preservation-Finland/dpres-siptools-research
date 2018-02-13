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
                "type": "object",
                "required": ["files", "provenance"],
                "properties": {
                    "provenance": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["type", "temporal", "description"],
                            "properties": {
                                "temporal": {
                                    "type": "object",
                                    "required": ["start_date"]
                                },
                                "description": {
                                    "type": "object",
                                    "required": ["en"]
                                },
                                "type": {
                                    "type": "object",
                                    "required": ["pref_label"],
                                    "properties": {
                                        "pref_label": {
                                            "type": "object",
                                            "required": ["en"]
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "files": {
                        "type": "array",
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
