"""Validation schemas for JSON metadata retrieved from Metax."""
import os
import json
import pkg_resources


SCHEMAS_PATH = pkg_resources.resource_filename('siptools_research', 'schemas')


# JSON schema for dataset metadata
with open(os.path.join(SCHEMAS_PATH, 'dataset.json')) as json_file:
    DATASET_METADATA_SCHEMA = json.load(json_file)

# JSON schema for file metadata
with open(os.path.join(SCHEMAS_PATH, 'file.json')) as json_file:
    FILE_METADATA_SCHEMA = json.load(json_file)
