# coding=utf-8
"""Module for generating test datasets at runtime."""
from copy import deepcopy

from tests.metax_data.files import get_file

BASE_DATASET = {
    "identifier": "dataset_id",
    "preservation_identifier": "doi:test",
    "contract": {
        "identifier": "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd"
    },
    "research_dataset": {
        "provenance": [
            {
                "preservation_event": {
                    "pref_label": {
                        "en": "creation"
                    }
                },
                "temporal": {
                    "end_date": "2014-12-31T08:19:58Z",
                    "start_date": "2014-01-01T08:19:58Z"
                },
                "description": {
                    "en": "Description of provenance"
                },
                'event_outcome': {
                    "pref_label": {
                        "en": "outcome"
                    }
                },
                'outcome_description': {
                    "en": "outcome_description"
                }
            }
        ],
        "files": [],
        "directories": []
    },
    "preservation_state": 0
}

DATASETS = {
    "1": {},
    "generate_metadata_test_dataset_1_ida": {
        "files": ["pid:urn:generate_metadata_1_ida"]
    },
    "generate_metadata_test_dataset_1_local": {
        "files": ["pid:urn:generate_metadata_1_local"]
    },
    "generate_metadata_test_dataset_2_ida": {
        "files": ["pid:urn:generate_metadata_2_ida"]
    },
    "generate_metadata_test_dataset_2_local": {
        "files": ["pid:urn:generate_metadata_2_local"]
    },
    "generate_metadata_test_dataset_3_ida": {
        "files": ["pid:urn:generate_metadata_3_ida"]
    },
    "generate_metadata_test_dataset_3_local": {
        "files": ["pid:urn:generate_metadata_3_local"]
    },
    "generate_metadata_test_dataset_4_ida": {
        "files": ["pid:urn:generate_metadata_4_ida"]
    },
    "generate_metadata_test_dataset_4_local": {
        "files": ["pid:urn:generate_metadata_4_local"]
    },
    "generate_metadata_test_dataset_5_ida": {
        "files": ["pid:urn:generate_metadata_5_ida"]
    },
    "generate_metadata_test_dataset_5_local": {
        "files": ["pid:urn:generate_metadata_5_local"]
    },
    "generate_metadata_test_dataset_file_characteristics_ida": {
        "files": ["pid:urn:generate_metadata_file_characteristics_ida"]
    },
    "generate_metadata_test_dataset_file_characteristics_local": {
        "files": ["pid:urn:generate_metadata_file_characteristics_local"]
    },
    "validate_metadata_test_dataset": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"]
    },
    "validate_metadata_en": {
        "set": [(
            "research_dataset", {
                "provenance": [
                    {
                        "preservation_event": {
                            "pref_label": {
                                "en": "creation"
                            }
                        },
                        "temporal": {
                            "end_date": "2014-12-31T08:19:58Z",
                            "start_date": "2014-01-01T08:19:58Z"
                        },
                        "description": {
                            "en": "Description of provenance"
                        },
                        "event_outcome": {
                            "pref_label": {
                                "en": "success"
                            }
                        },
                        "outcome_description": {
                            "en": "This is a detail of an successful event"
                        }
                    }
                ],
                "files": []
            }
        )]
    },
    "validate_metadata_fi": {
        "set": [(
            "research_dataset", {
                "provenance": [
                    {
                        "preservation_event": {
                            "pref_label": {
                                "fi": "luonti"
                            }
                        },
                        "temporal": {
                            "end_date": "2014-12-31T08:19:58Z",
                            "start_date": "2014-01-01T08:19:58Z"
                        },
                        "description": {
                            "fi": u"Alkuperän kuvaus"
                        },
                        "event_outcome": {
                            "pref_label": {
                                "fi": u"läpi"
                            }
                        },
                        "outcome_description": {
                            "fi": "Kuvaus tapahtumasta"
                        }
                    }
                ],
                "files": []
            }
        )]
    },
    "validate_metadata_enfi": {
        "set": [(
            "research_dataset", {
                "provenance": [
                    {
                        "preservation_event": {
                            "pref_label": {
                                "en": "creation",
                                "fi": "luonti"
                            }
                        },
                        "temporal": {
                            "end_date": "2014-12-31T08:19:58Z",
                            "start_date": "2014-01-01T08:19:58Z"
                        },
                        "description": {
                            "en": "Description of provenance",
                            "fi": u"Alkuperän kuvaus"
                        },
                        "event_outcome": {
                            "pref_label": {
                                "en": "success",
                                "fi": u"läpi"
                            }
                        },
                        "outcome_description": {
                            "en": "This is a detail of an successful event",
                            "fi": "Kuvaus tapahtumasta"
                        }
                    }
                ],
                "files": []
            }
        )]
    },
    "validate_metadata_localization_missing": {
        "set": [(
            "research_dataset", {
                "provenance": [
                    {
                        "preservation_event": {
                            "pref_label": {}
                        },
                        "temporal": {
                            "end_date": "2014-12-31T08:19:58Z",
                            "start_date": "2014-01-01T08:19:58Z"
                        },
                        "description": {},
                        "event_outcome": {
                            "pref_label": {}
                        },
                        "outcome_description": {}
                    }
                ],
                "files": []
            }
        )]
    },
    "validate_metadata_test_dataset_invalid_metadata": {
        "remove": ["contract"]
    },
    "validate_invalid_file_type_": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:invalid_file_type_"]
    },
    "validate_invalid_file_type_1.0": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:invalid_file_type_1.0"]
    },
    "validate_metadata_test_dataset_invalid_contract_metadata": {
        "contract": "contract_with_invalid_metadata"
    },
    "validate_metadata_test_dataset_invalid_file_path": {
        "files": ["pid:urn:invalidpath"]
    },
    "validate_metadata_test_dataset_metadata_missing": {
        "files": ["pid:urn:validate_metadata_test_image"]
    },
    "validate_metadata_test_dataset_audio_video_metadata": {
        "files": ["pid:urn:891", "pid:urn:892"]
    },
    "validate_metadata_test_dataset_invalid_audiomd": {
        "files": ["pid:urn:testaudio"]
    },
    "validate_metadata_test_dataset_corrupted_mix": {
        "files": ["pid:urn:testimage"]
    },
    "validate_metadata_test_dataset_invalid_datacite": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"]
    },
    "validate_metadata_test_dataset_corrupted_datacite": {},
    "validate_metadata_test_dataset_publisher_missing": {},
    "validate_metadata_test_dataset_missing_file_format": {
        "files": ["pid:urn:wf_test_1a_ida_missing_file_format"]
    },
    "validate_metadata_test_dataset_very_invalid_datacite": {},
    "create_digiprov_test_dataset_file_and_logging": {
        "provenance": ["creation", "metadata modification"]
    },
    "validate_files_valid": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"]
    },
    "validate_files_invalid": {
        "files": ["pid:urn:invalid_mimetype_1", "pid:urn:invalid_mimetype_2"]
    },
    "create_digiprov_test_dataset_date_data_missing": {
        "set": [(
            "research_dataset", {
                "provenance": [{
                    "preservation_event": {
                        "pref_label": {
                            "en": "creation"
                        }
                    },
                    "description": {
                        "en": "Description of provenance"
                    }
                }]
            }
        )]
    },
    "create_digiprov_test_dataset_detailed_check": {
        "set": [(
            "research_dataset", {
                "provenance": [
                    {
                        "preservation_event": {
                            "pref_label": {
                                "en": "creation"
                            }
                        },
                        "temporal": {
                            "end_date": "2014-12-31T08:19:58Z",
                            "start_date": "2014-01-01T08:19:58Z"
                        },
                        "description": {
                            "en": "Description of provenance"
                        },
                        "event_outcome": {
                            "pref_label": {
                                "en": "success"
                            }
                        },
                        "outcome_description": {
                            "en": "This is a detail of an successful event"
                        }
                    }
                ]
            }
        )]
    },
    "create_digiprov_test_dataset_provenance_data_missing": {
        "set": [(
            "research_dataset", {
                "files": [{
                    "identifier": "pid:urn:892",
                    "use_category": {
                        "pref_label": {
                            "en": "label5"
                        }
                    }
                }]
            }
        )]
    },
    "datacite_test_1": {},
    "create_structmap_test_dataset": {
        "files_with_labels": [
            ("pid:urn:1", "Access and use rights files"),
            ("pid:urn:2", "Documentation files"),
            ("pid:urn:3", "Documentation files"),
            ("pid:urn:4", "Documentation files"),
            ("pid:urn:5", "Documentation files"),
            ("pid:urn:6", "Documentation files"),
            ("pid:urn:7", "Machine-readable metadata"),
            ("pid:urn:8", "Publication files"),
            ("pid:urn:9", "Software files")
        ],
        "directories": [
            ("pid:urn:dir:1", "Access and use rights files"),
            ("pid:urn:dir:2", "Documentation files"),
            ("pid:urn:dir:3", "Machine-readable metadata"),
            ("pid:urn:dir:4", "Publication files"),
            ("pid:urn:dir:5", "Software files")
        ]
    },
    "get_files_test_dataset_ida": {
        "files": ["pid:urn:get_files_1", "pid:urn:get_files_2"]
    },
    "get_files_test_dataset_local": {
        "files": ["pid:urn:get_files_1_local", "pid:urn:get_files_2_local"]
    },
    "get_files_test_dataset_ida_missing_file": {
        "files": ["pid:urn:get_files_1", "pid:urn:does_not_exist"]
    },
    "get_files_test_dataset_local_missing_file": {
        "files": ["pid:urn:get_files_1_local", "pid:urn:does_not_exist_local"]
    },
    "create_techmd_test_dataset": {
        "files": [
            "pid:urn:techmd_5",
            "pid:urn:techmd_6",
            "pid:urn:techmd_7",
            "pid:urn:techmd_999",
            "pid:urn:techmd_888"
        ]
    },
    "create_techmd_test_dataset_charset_not_defined": {
        "files": ["pid:urn:create_techmd_3"]
    },
    "create_techmd_test_dataset_xml_metadata_missing": {
        "files": ["pid:urn:create_techmd_8"]
    },
    "create_mets_dataset": {
        "contract": "urn:uuid:99ddffff-2f73-46b0-92d1-614409d83001",
        "remove": ["research_dataset"]
    },
    "report_preservation_status_test_dataset_ok": {},
    "report_preservation_status_test_dataset_rejected": {
        "set": [(
            "research_dataset", {
                "rights_holder": {
                    "name": {
                        "und": "Helsingin yliopisto"
                    },
                    "@type": "Organization",
                    "email": "right.holder@company.com",
                    "telephone": ["+358501231235"],
                    "identifier": "http://purl.org/att/es/organization_data/"
                                  "organization/organization_1901",
                    "is_part_of": {
                        "name": {
                            "und": "Aalto yliopisto"
                        },
                        "@type": "Organization",
                        "identifier": "http://purl.org/att/es/organization"
                                      "_data/organization/organization_10076"
                    }
                }
            }
        )]
    },
    "workflow_test_dataset_1_ida": {
        "files": ["pid:urn:wf_test_1a_ida", "pid:urn:wf_test_1b_ida"]
    },
    "workflow_test_dataset_1_local": {
        "files": ["pid:urn:wf_test_1a_local", "pid:urn:wf_test_1b_local"]
    },
    "missing_csv_info": {
        "files": ["missing_csv_info"]
    },
    "missing_provenance": {
        "set": [("research_dataset", {"files": []})]
    },
    "empty_provenance": {
        "set": [("research_dataset", {"files": [], "provenance":[]})]
    },
    "dataset_1": {
    },
    "dataset_1_in_packaging_service_with_conflicting_description": {
        "set": [("preservation_state", 90),
                ("preservation_description", "foobar")]
    },
    "dataset_1_in_packaging_service": {
        "set": [("preservation_state", 90),
                ("preservation_description", "In packaging service")]
    }
}


def get_dataset(self, dataset_id):
    """Generate and return dataset with identifier dataset_id"""
    new_dataset = deepcopy(BASE_DATASET)
    new_dataset["identifier"] = dataset_id
    dataset = DATASETS[dataset_id]

    # Add files to dataset
    if "files" in dataset:
        for _file in dataset["files"]:
            files = new_dataset["research_dataset"]["files"]
            files.append({
                "identifier": _file,
                "use_category": {
                    "pref_label": {
                        "en": "label2"
                    }
                }
            })

    # Add files with specified labels to dataset
    if "files_with_labels" in dataset:
        for _file, label in dataset["files_with_labels"]:
            files = new_dataset["research_dataset"]["files"]
            files.append({
                "identifier": _file,
                "use_category": {
                    "pref_label": {
                        "en": label
                    }
                }
            })

    # Set directories to dataset
    if "directories" in dataset:
        for directory, label in dataset["directories"]:
            directories = new_dataset["research_dataset"]["directories"]
            directories.append({
                "identifier": directory,
                "use_category": {
                    "pref_label": {
                        "en": label
                    }
                }
            })

    # Set contract identifier
    if "contract" in dataset:
        new_dataset["contract"]["identifier"] = dataset["contract"]

    if "provenance" in dataset:
        base_provenance = new_dataset["research_dataset"].pop("provenance")[0]
        new_dataset["research_dataset"]["provenance"] = []

        for event in dataset["provenance"]:
            provenance = deepcopy(base_provenance)
            provenance["preservation_event"]["pref_label"]["en"] = event
            new_dataset["research_dataset"]["provenance"].append(provenance)

    # Set arbitrary field
    if "set" in dataset:
        for params in dataset["set"]:
            key = params[0]
            value = params[1]
            new_dataset[key] = value

    # Delete keys that exist in the BASE_DATASET
    if "remove" in dataset:
        for key in dataset["remove"]:
            if key in new_dataset:
                new_dataset.pop(key)

    return new_dataset


def get_dataset_files(self, dataset_id):
    """Get a list of all file metadata of a given dataset."""
    dataset = get_dataset(self, dataset_id)
    files = []

    for _file in dataset["research_dataset"]["files"]:
        file_id = _file["identifier"]
        files.append(get_file(self, file_id))

    return files
