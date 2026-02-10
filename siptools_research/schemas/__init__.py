"""Validation schemas for JSON metadata retrieved from Metax."""
import json
from importlib.resources import path as resource_path

SCHEMAS_PATH = 'siptools_research.schemas'


# JSON schema for dataset metadata
with resource_path(SCHEMAS_PATH, 'dataset.json') as json_file:
    DATASET_METADATA_SCHEMA = json.load(json_file.open())

# JSON schema for file metadata
with resource_path(SCHEMAS_PATH, 'file.json') as json_file:
    FILE_METADATA_SCHEMA = json.load(json_file.open())
