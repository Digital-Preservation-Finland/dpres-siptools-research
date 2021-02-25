"""Module for generating test files at runtime."""
from copy import deepcopy

import lxml.etree

BASE_ADDML_MD = lxml.etree.parse('tests/data/addml_sample.xml')
BASE_AUDIO_MD = lxml.etree.parse('tests/data/audiomd_sample.xml')
BASE_VIDEO_MD = lxml.etree.parse('tests/data/videomd_sample.xml')


PAS_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"

CSV_FILE_CHARS = {
    "file_created": "2014-01-17T08:19:31Z",
    "file_format": "text/csv",
    "format_version": "",
    "encoding": "UTF-8",
    "csv_delimiter": ";",
    "csv_record_separator": "CR+LF",
    "csv_quoting_char": "'",
    "csv_has_header": False
}

TXT_FILE_CHARS = {
    "encoding": "UTF-8",
    "file_created": "2014-01-17T08:19:31Z",
    "file_format": "text/plain"
}

TIFF_FILE_CHARS = {
    "file_created": "2018-01-17T08:19:31Z",
    "file_format": "image/tiff",
    "format_version": "6.0"
}

HTML_FILE_CHARS = {
    "file_format": "text/html",
    "format_version": "5.0",
    "encoding": "UTF-8"
}

XML_FILE_CHARS = {
    "file_format": "text/xml",
    "format_version": "1.0",
    "encoding": "UTF-8"
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

TXT_FILE = deepcopy(BASE_FILE)
TXT_FILE['file_characteristics'] = TXT_FILE_CHARS

TIFF_FILE = deepcopy(BASE_FILE)
TIFF_FILE['file_characteristics'] = TIFF_FILE_CHARS

CSV_FILE = deepcopy(BASE_FILE)
CSV_FILE['file_characteristics'] = CSV_FILE_CHARS


FILES = {
    "pid:urn:textfile1": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file1"),
            ("checksum", {
                "algorithm": "md5",
                "value": "58284d6cdd8deaffe082d063580a9df3"
            })
        ]
    },
    "pid:urn:textfile2": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file2"),
            ("checksum", {
                "algorithm": "md5",
                "value": "90dd1fc82b5d523f6f85716c1c67c0f3"
            })
        ]
    },
    "pid:urn:invalid_mimetype_1": {
        "file_characteristics": TIFF_FILE_CHARS
    },
    "pid:urn:invalid_mimetype_2": {
        "file_characteristics": TIFF_FILE_CHARS
    },
    "pid:urn:not_found_1": {
        "file_characteristics": TIFF_FILE_CHARS
    },
    "pid:urn:not_found_2": {
        "file_characteristics": TIFF_FILE_CHARS
    },
    "pid:urn:get_files_1": {
        "set": [
            ("file_path", "/path/to/file1"),
            ("identifier", "pid:urn:1")
        ]
    },
    "pid:urn:get_files_1_local": {
        "set": [("file_path", "/path/to/file1")]
    },
    "pid:urn:get_files_2": {
        "set": [
            ("file_path", "/path/to/file2"),
            ("identifier", "pid:urn:2")
        ]
    },
    "pid:urn:get_files_2_local": {
        "set": [("file_path", "/path/to/file2")]
    },
    "pid:urn:does_not_exist": {
        "set": [("file_path", "/path/to/file4")]
    },
    "pid:urn:does_not_exist_local": {
        "set": [("file_path", "/path/to/file4")]
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
        for params in _file["set"]:
            key = params[0]
            value = params[1]
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
