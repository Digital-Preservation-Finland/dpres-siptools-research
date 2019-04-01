"""Validation schemas for JSON metadata retrieved from Metax."""

# JSON schema for dataset metadata
# http://json-schema.org/
DATASET_METADATA_SCHEMA = {
    "type": "object",
    "required": ["research_dataset", "contract", "preservation_identifier"],
    "properties": {
        "preservation_identifier": {
            "type": "string"
        },
        "contract": {
            "type": "object",
            "required": ["identifier"]
        },
        "research_dataset": {
            "type": "object",
            "required": ["files"],
            "properties": {
                "provenance": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "preservation_event",
                            "temporal",
                            "description"
                        ],
                        "properties": {
                            "temporal": {
                                "type": "object",
                                "required": ["start_date"]
                            },
                            "description": {
                                "type": "object",
                                "required": ["en"]
                            },
                            "preservation_event": {
                                "type": "object",
                                "required": ["identifier", "pref_label"],
                                "properties": {
                                    "identifier": {
                                        "type": "string"
                                    },
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
                                                "type": "string"
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
FILE_METADATA_SCHEMA = {
    "type": "object",
    "required": ["checksum", "file_characteristics", "file_storage",
                 "file_path", "parent_directory"],
    "properties": {
        "checksum": {
            "type": "object",
            "required": ["algorithm", "value"],
            "properties": {
                "algorithm": {
                    "type": "string",
                    "enum": ['md5', 'sha2']
                }
            }
        },
        "file_characteristics": {
            "type": "object",
            "required": ["file_format"],
            "properties": {
                "file_encoding": {
                    "type": "string",
                    "enum": ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']
                }
            }
        },
        "file_storage": {
            "type": "object",
            "required": ["identifier"]
        },
        "parent_directory": {
            "type": "object",
            "required": ["identifier"]
        }
    }
}


# JSON schema for contract metadata
CONTRACT_METADATA_SCHEMA = {
    "type": "object",
    "required": ["contract_json"],
    "properties": {
        "contract_json": {
            "type": "object",
            "required": ["identifier", "organization"],
            "properties": {
                "identifier": {
                    "type": "string"
                },
                "organization": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}
