# coding=utf-8
"""Module for generating test datasets at runtime."""
from copy import deepcopy

import lxml.etree

from tests.metax_data.files import get_file

BASE_DATACITE = lxml.etree.parse('tests/data/datacite_sample.xml')

BASE_PROVENANCE = {
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

BASE_DATASET = {
    "identifier": "dataset_identifier",
    "preservation_identifier": "doi:test",
    "contract": {
        "identifier": "contract_identifier"
    },
    "research_dataset": {
        "provenance": [BASE_PROVENANCE],
        "files": [],
        "directories": []
    },
    "preservation_state": 0
}

DATASETS = {
    "1": {},
    "validate_metadata_test_dataset": {
        "files": ["pid:urn:textfile1", "pid:urn:textfile2"]
    },
    "validate_metadata_test_dataset_invalid_metadata": {
        "remove": ["contract"]
    },
    "create_digiprov_test_dataset_file_and_logging": {
        "provenance": ["creation", "metadata modification"]
    },
    "validate_files_valid": {
        "files": ["pid:urn:textfile1", "pid:urn:textfile2"]
    },
    "validate_files_invalid": {
        "files": [
            "pid:urn:invalid_mimetype_1",
            "pid:urn:invalid_mimetype_2"
        ]
    },
    "validate_files_not_found": {
        "files": [
            "pid:urn:not_found_1",
            "pid:urn:not_found_2"
        ]
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
    "get_files_test_dataset": {
        "files": ["pid:urn:get_files_1", "pid:urn:get_files_2"]
    },
    "get_files_test_dataset_ida_missing_file": {
        "files": ["pid:urn:get_files_1", "pid:urn:does_not_exist"]
    },
    "get_files_test_dataset_local_missing_file": {
        "files": ["pid:urn:get_files_1_local", "pid:urn:does_not_exist_local"]
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


def get_dataset(_self, dataset_id):
    """Generate and return dataset with identifier dataset_id."""
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
