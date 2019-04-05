"""Module for generating test files at runtime."""
from copy import deepcopy

PAS_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"

BASE_FILE = {
    "identifier": "pid:urn:identifier",
    "file_path": "path/to/file",
    "file_storage": {
        "identifier": "urn:nbn:fi:att:file-storage-ida"
    },
    "parent_directory": {
        "identifier": "pid:urn:dir:wf1"
    },
    "checksum": {
        "algorithm": "md5",
        "value": "58284d6cdd8deaffe082d063580a9df3"
    }
}

FILES = {
    "pid:urn:generate_metadata_1_ida": {
        "file_characteristics": {
            "encoding": "user_defined_charset",
            "file_created": "2014-01-17T08:19:31Z",
            "file_format": "text/plain",
            "dummy_key": "dummy_value"
        }
    },
    "pid:urn:generate_metadata_1_local": {
        "file_characteristics": {
            "encoding": "user_defined_charset",
            "file_created": "2014-01-17T08:19:31Z",
            "file_format": "text/plain",
            "dummy_key": "dummy_value"
        }
    },
    "pid:urn:generate_metadata_2_ida": {},
    "pid:urn:generate_metadata_2_local": {},
    "pid:urn:generate_metadata_3_ida": {
        "file_characteristics": {
            "file_created": "2014-01-17T08:19:31Z",
            "file_format": "text/csv",
            "format_version": "8.3",
            "encoding": "UTF-8",
            "csv_delimiter": ";",
            "csv_record_separator": "CR+LF",
            "csv_quoting_char": "'",
            "csv_has_header": False
        }
    },
    "pid:urn:generate_metadata_3_local": {
        "file_characteristics": {
            "file_created": "2014-01-17T08:19:31Z",
            "file_format": "text/csv",
            "format_version": "8.3",
            "encoding": "UTF-8",
            "csv_delimiter": ";",
            "csv_record_separator": "CR+LF",
            "csv_quoting_char": "'",
            "csv_has_header": False
        }
    },
    "pid:urn:generate_metadata_4_ida": {},
    "pid:urn:generate_metadata_4_local": {},
    "pid:urn:generate_metadata_5_ida": {},
    "pid:urn:generate_metadata_5_local": {},
    "pid:urn:generate_metadata_file_characteristics_ida": {},
    "pid:urn:generate_metadata_file_characteristics_local": {}
}


def get_file(self, file_id):
    """Generate and return file with identifier file_id"""
    new_file = deepcopy(BASE_FILE)
    new_file["identifier"] = file_id

    if "file_path" in FILES[file_id]:
        new_file["file_path"] = FILES[file_id]["file_path"]

    if "file_characteristics" in FILES[file_id]:
        file_chars = FILES[file_id]["file_characteristics"]
        new_file["file_characteristics"] = file_chars 

    if file_id.endswith("_local"):
        new_file["file_storage"]["identifier"] = PAS_STORAGE_ID

    return new_file
