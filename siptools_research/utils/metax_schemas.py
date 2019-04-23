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
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "type": "string"
                }
            }
        },
        "research_dataset": {
            "type": "object",
            "required": ["provenance"],
            "anyOf": [
                {"required": ["files"]},
                {"required": ["directories"]}
            ],
            "properties": {
                "provenance": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": [
                            "preservation_event",
                            "description",
                            "event_outcome",
                            "outcome_description"
                        ],
                        "properties": {
                            "temporal": {
                                "type": "object",
                                "required": ["start_date"],
                                "properties": {
                                    "start_date": {
                                        "type": "string"
                                    }
                                }
                            },
                            "description": {
                                "type": "object",
                                "required": ["en"],
                                "properties": {
                                    "en": {
                                        "type": "string"
                                    }
                                }
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
                                        "required": ["en"],
                                        "properties": {
                                            "en": {
                                                "type": "string"
                                            }
                                        }
                                    }
                                }
                            },
                            "event_outcome": {
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
                            },
                            "outcome_description": {
                                "type": "object",
                                "required": ["en"],
                                "properties": {
                                    "properties": {
                                        "en": {
                                            "type": "string"
                                        }
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
                },
                "directories": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["identifier", "use_category"],
                        "properties": {
                            "identfier": {
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
                },
                "value": {
                    "type": "string"
                }
            }
        },
        "file_path": {
            "type": "string"
        },
        "file_characteristics": {
            "type": "object",
            "required": ["file_format"],
            "properties": {
                "file_encoding": {
                    "type": "string",
                    "enum": ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']
                },
                "format_version": {
                    "type": "string"
                },
                "file_created": {
                    "type": "string"
                },
                "file_format": {
                    "type": "string"
                }
            }
        },
        "file_storage": {
            "type": "object",
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "type": "string"
                }
            }
        },
        "parent_directory": {
            "type": "object",
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "type": "string"
                }
            }
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
