"""Validation schemas for JSON metadata retrieved from Metax."""

# JSON schema for dataset metadata
# http://json-schema.org/
DATASET_METADATA_SCHEMA = {
    "type": "object",
    "required": ["research_dataset", "contract", "preservation_identifier"],
    "properties": {
        "preservation_identifier": {
            "title": "Preservation identifier",
            "description": "Package identifier i.e. the OBJID attribute in "
                           "METS document",
            "type": "string"
        },
        "contract": {
            "title": "Contract",
            "description": "Metadata for identifying contract",
            "type": "object",
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "title": "Identifier",
                    "description": "Identifier of contract in Metax",
                    "type": "string"
                }
            }
        },
        "research_dataset": {
            "title": "Research dataset",
            "description": "Administrative metadata and files included in "
                           "dataset",
            "type": "object",
            "required": ["provenance"],
            "anyOf": [
                {"required": ["files"]},
                {"required": ["directories"]}
            ],
            "properties": {
                "provenance": {
                    "title": "Provenance",
                    "description": "Digital provenance metadata. Packaging "
                                   "service can create dummy provenance "
                                   "metadata if it is not provided.",
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
                                "title": "Temporal",
                                "description": "Creation date of dataset",
                                "type": "object",
                                "required": ["start_date"],
                                "properties": {
                                    "start_date": {
                                        "title": "Start date",
                                        "description": "The value of "
                                                       "premis:eventDateTime "
                                                       "element",
                                        "type": "string"
                                    }
                                }
                            },
                            "description": {
                                "title": "Description",
                                "description": "The description of digital "
                                               "provenance event in multiple "
                                               "languages. The description in "
                                               "the default language of the "
                                               "dataset is used as value of "
                                               "premis:eventDetail element.",
                                "type": "object",
                                "required": ["en"],
                                "properties": {
                                    "en": {
                                        "title": "en",
                                        "description": "English",
                                        "type": "string"
                                    }
                                }
                            },
                            "preservation_event": {
                                "title": "Preservation event",
                                "description": "The digital provenance event",
                                "type": "object",
                                "required": ["pref_label"],
                                "properties": {
                                    "pref_label": {
                                        "title": "Preferred label",
                                        "description": "The type of digital"
                                                       "provenance event in "
                                                       "multiple languages. "
                                                       "The type in the "
                                                       "default language of "
                                                       "the dataset is used as"
                                                       " value of "
                                                       "premis:eventType "
                                                       "element.",
                                        "type": "object",
                                        "required": ["en"],
                                        "properties": {
                                            "en": {
                                                "title": "en",
                                                "description": "English",
                                                "type": "string"
                                            }
                                        }
                                    }
                                }
                            },
                            "event_outcome": {
                                "title": "Event outcome",
                                "description": "Digital provenance event "
                                               "outcome information",
                                "type": "object",
                                "required": ["pref_label"],
                                "properties": {
                                    "pref_label": {
                                        "title": "Preferred label",
                                        "description": "The outcome of digital"
                                                       " provenance event in "
                                                       "multiple languages. "
                                                       "The outcome in the "
                                                       "default language of "
                                                       "the dataset is used as"
                                                       " value of "
                                                       "premis:eventOutcomeInf"
                                                       "ormation element",
                                        "type": "object",
                                        "required": ["en"],
                                        "properties": {
                                            "en": {
                                                "title": "en",
                                                "description": "English",
                                                "type": "string"
                                            }
                                        }
                                    }
                                }
                            },
                            "outcome_description": {
                                "title": "Outcome description",
                                "description": "The outcome detail note of "
                                               "digital provenance event in "
                                               "multiple languages. The "
                                               "outcome detail note in the "
                                               "default language of the "
                                               "dataset is used as value of "
                                               "premis:eventOutcomeDetailNote "
                                               "element",
                                "type": "object",
                                "required": ["en"],
                                "properties": {
                                    "properties": {
                                        "en": {
                                            "title": "en",
                                            "description": "English",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "files": {
                    "title": "Files",
                    "description": "List of files included in the dataset",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["identifier", "use_category"],
                        "properties": {
                            "identifier": {
                                "title": "Identifier",
                                "description": "The identifier used for "
                                               "finding file metadata from "
                                               "Metax and the file itself "
                                               "from Ida download service.",
                                "type": "string"
                            },
                            "use_category": {
                                "title": "Use category",
                                "description": "The category of file",
                                "type": "object",
                                "required": ["pref_label"],
                                "properties": {
                                    "pref_label": {
                                        "title": "Preferred label",
                                        "description": "The category of file "
                                                       "in logical structure "
                                                       "map in multiple "
                                                       "languages. The "
                                                       "category in the "
                                                       "default language of "
                                                       "the dataset is used "
                                                       "as TYPE attribute for"
                                                       " div element in "
                                                       "logical structmap.",
                                        "type": "object",
                                        "required": ["en"],
                                        "properties": {
                                            "en": {
                                                "title": "en",
                                                "description": "English",
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
                    "title": "Directories",
                    "description": "List of directories included in dataset",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["identifier", "use_category"],
                        "properties": {
                            "identfier": {
                                "title": "Identifier",
                                "description": "Identifier of category used "
                                               "for searching parent "
                                               "directories of files",
                                "type": "string"
                            },
                            "use_category": {
                                "title": "Use category",
                                "description": "The category of directory",
                                "type": "object",
                                "required": ["pref_label"],
                                "properties": {
                                    "pref_label": {
                                        "title": "Preferred label",
                                        "description": "The category of "
                                                       "directory files in "
                                                       "logical structure map."
                                                       " If use category is "
                                                       "not defined for a "
                                                       "file, the use category"
                                                       " of parent directory "
                                                       "is used as TYPE "
                                                       "attribute for div "
                                                       "element inlogical "
                                                       "structmap.",
                                        "type": "object",
                                        "required": ["en"],
                                        "properties": {
                                            "en": {
                                                "title": "en",
                                                "description": "English",
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
            "title": "Checksum",
            "description": "File checksum algorithm and value",
            "type": "object",
            "required": ["algorithm", "value"],
            "properties": {
                "algorithm": {
                    "title": "Algorithm",
                    "description": "Checksum algorithm is md5 or sha2. "
                                   "If sha2 is used, packaging service will "
                                   "guess which function (SHA-224, SHA-256, "
                                   "SHA-384, or SHA-512) was used. Checksum "
                                   "algorithm is used as value of "
                                   "premis:messageDigestAlgorithm "
                                   "element in technical metadata.",
                    "type": "string",
                    "enum": ['md5', 'sha2']
                },
                "value": {
                    "title": "Value",
                    "description": "Output of checksum algorithm",
                    "type": "string"
                }
            }
        },
        "file_path": {
            "title": "File path",
            "description": "Path of file in SIP",
            "type": "string"
        },
        "file_characteristics": {
            "title": "File characteristics",
            "description": "Technical characteristics of file",
            "type": "object",
            "required": ["file_format"],
            "properties": {
                "file_encoding": {
                    "title": "File encoding",
                    "description": "Character encoding of file is included in "
                                   "premis:formatName element in technical "
                                   "metadata.",
                    "type": "string",
                    "enum": ['ISO-8859-15', 'UTF-8', 'UTF-16', 'UTF-32']
                },
                "format_version": {
                    "title": "Format version",
                    "description": "Format version is used as value for "
                                   "premis:formatVersion element in technical "
                                   "metadata.",
                    "type": "string"
                },
                "file_created": {
                    "title": "File created",
                    "description": "File creation date is used as value of "
                                   "premis:dateCreatedByApplication element "
                                   "in technical metadata. If value is not "
                                   "provided, the file creation date in "
                                   "packaging service is used.",
                    "type": "string"
                },
                "file_format": {
                    "title": "File format",
                    "description": "File format is used as value of "
                                   "premis:formatName element in technical "
                                   "metadata.",
                    "type": "string"
                }
            }
        },
        "file_storage": {
            "title": "File storage",
            "description": "File storage service",
            "type": "object",
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "title": "Identifier",
                    "description": "Identifier that defines from which storage"
                                   " service the file is copied.",
                    "type": "string"
                }
            }
        },
        "parent_directory": {
            "title": "Parent directory",
            "description": "Parent directory of file defines the category of "
                           "file in logical structure map if use category is "
                           "not defined for the file.",
            "type": "object",
            "required": ["identifier"],
            "properties": {
                "identifier": {
                    "title": "Identifier",
                    "description": "Identifier used for finding parent "
                                   "directory metadata",
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
            "title": "Contract JSON",
            "description": "DP Service contract metadata",
            "type": "object",
            "required": ["identifier", "organization"],
            "properties": {
                "identifier": {
                    "title": "Identifier",
                    "description": "Contract identifier is used as value of "
                                   "fi:CONTRACTID attribute in METS document.",
                    "type": "string"
                },
                "organization": {
                    "title": "Organization",
                    "description": "Organization used as archivist "
                                   "organization agent in METS document.",
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {
                            "title": "Name",
                            "description": "Name used as value of mets:name "
                                           "element in METS agent.",
                            "type": "string"
                        }
                    }
                }
            }
        }
    }
}
