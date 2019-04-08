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
                    "identifier": "1234",
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
                }
            }
        ],
        "files": []
    }
}

DATASETS = {
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
    "validate_metadata_test_dataset_invalid_metadata": {
        "remove": ["contract"]
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

    # Set contract identifier
    if "contract" in dataset:
        new_dataset["contract"]["identifier"] = dataset["contract"]

    # Set arbitrary field
    if "set" in dataset:
        for key, value in dataset["set"]:
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
