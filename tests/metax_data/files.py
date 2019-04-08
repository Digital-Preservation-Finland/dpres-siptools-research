"""Module for generating test files at runtime."""
from copy import deepcopy

PAS_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"

CSV_FILE_CHARS = {
    "file_created": "2014-01-17T08:19:31Z",
    "file_format": "text/csv",
    "format_version": "8.3",
    "encoding": "UTF-8",
    "csv_delimiter": ";",
    "csv_record_separator": "CR+LF",
    "csv_quoting_char": "'",
    "csv_has_header": False
}

TXT_FILE_CHARS = {
    "encoding": "utf-8",
    "file_created": "2014-01-17T08:19:31Z",
    "file_format": "text/plain"
}

TIFF_FILE_CHARS = {
    "file_created": "2018-01-17T08:19:31Z",
    "file_format": "image/tiff",
    "format_version": "6.0"
}

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
        "file_characteristics": CSV_FILE_CHARS
    },
    "pid:urn:generate_metadata_3_local": {
        "file_characteristics": CSV_FILE_CHARS
    },
    "pid:urn:generate_metadata_4_ida": {},
    "pid:urn:generate_metadata_4_local": {},
    "pid:urn:generate_metadata_5_ida": {},
    "pid:urn:generate_metadata_5_local": {},
    "pid:urn:generate_metadata_file_characteristics_ida": {},
    "pid:urn:generate_metadata_file_characteristics_local": {},
    "pid:urn:wf_test_1a_ida": {
        "file_characteristics": TXT_FILE_CHARS
    },
    "pid:urn:wf_test_1a_local": {
        "file_characteristics": TXT_FILE_CHARS
    },
    "pid:urn:wf_test_1b_ida": {
        "file_characteristics": TXT_FILE_CHARS
    },
    "pid:urn:wf_test_1b_local": {
        "file_characteristics": TXT_FILE_CHARS
    },
    "pid:urn:invalidpath": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [("file_path", "../../file_in_invalid_path")]
    },
    "pid:urn:validate_metadata_test_image": {
        "file_characteristics": TIFF_FILE_CHARS
    },
    "pid:urn:891": {
        "file_characteristics": {
            "file_created": "2018-01-17T08:19:31Z",
            "file_format": "audio/mp4"
        }
    },
    "pid:urn:892": {
        "file_characteristics": {
            "file_created": "2018-01-17T08:19:31Z",
            "file_format": "video/mp4"
        }
    },
    "pid:urn:testaudio": {
        "file_characteristics": {
            "file_format": "audio/mpeg"
        }
    },
    "pid:urn:testimage": {
        "file_characteristics": TIFF_FILE_CHARS
    }
}


def get_file(self, file_id):
    """Generate and return file with identifier file_id"""
    new_file = deepcopy(BASE_FILE)
    new_file["identifier"] = file_id
    _file = FILES[file_id]

    # Set file_characteristics field
    if "file_characteristics" in FILES[file_id]:
        file_chars = FILES[file_id]["file_characteristics"]
        new_file["file_characteristics"] = file_chars

    # Set arbitrary field
    if "set" in _file:
        for key, value in _file["set"]:
            new_file[key] = value

    # Remove field that exist in the BASE_FILE
    if "remove" in _file:
        for key in _file["remove"]:
            if key in new_file:
                new_file.pop(key)

    # Change file_storage identifier for the upload-rest-api test cases
    if file_id.endswith("_local"):
        new_file["file_storage"]["identifier"] = PAS_STORAGE_ID

    return new_file
