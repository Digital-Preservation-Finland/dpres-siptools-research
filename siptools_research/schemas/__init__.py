"""Validation schemas for JSON metadata retrieved from Metax."""
import json
import os
import pkg_resources
import sys


SCHEMAS_PATH = pkg_resources.resource_filename('siptools_research', 'schemas')


# TODO: This hack is required because in Python2 json.load decodes all strings
# to unicode. In Python3 it should not cause any problems, and json.load can be
# used without the object_hook
def _byteify(data):
    """Recursively convert unicode data to str.

    :param data: unicode data as list, string, or dictionary
    :returns: data converted to str
    """
    # Python3 compatibility
    if sys.version_info >= (3, 0):
        return data

    if isinstance(data, unicode):
        return data.encode('utf-8')

    if isinstance(data, list):
        return [_byteify(item) for item in data]

    if isinstance(data, dict):
        return {
            _byteify(key): _byteify(value)
            for key, value in data.iteritems()
        }

    # if it's anything else, return it in its original form
    return data


# JSON schema for dataset metadata
with open(os.path.join(SCHEMAS_PATH, 'dataset.json')) as json_file:
    DATASET_METADATA_SCHEMA = json.load(json_file, object_hook=_byteify)

# JSON schema for file metadata
with open(os.path.join(SCHEMAS_PATH, 'file.json')) as json_file:
    FILE_METADATA_SCHEMA = json.load(json_file, object_hook=_byteify)

# JSON schema for contract metadata
with open(os.path.join(SCHEMAS_PATH, 'contract.json')) as json_file:
    CONTRACT_METADATA_SCHEMA = json.load(json_file, object_hook=_byteify)
