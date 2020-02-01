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
    "format_version": "4.01",
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
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file1"),
            ("checksum", {
                "algorithm": "md5",
                "value": "58284d6cdd8deaffe082d063580a9df3"
            })
        ]
    },
    "pid:urn:wf_test_1a_local": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file1"),
            ("checksum", {
                "algorithm": "md5",
                "value": "58284d6cdd8deaffe082d063580a9df3"
            })
        ]
    },
    "pid:urn:wf_test_1b_ida": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file2"),
            ("checksum", {
                "algorithm": "md5",
                "value": "90dd1fc82b5d523f6f85716c1c67c0f3"
            })
        ]
    },
    "pid:urn:wf_test_1b_local": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("file_path", "/path/to/file2"),
            ("checksum", {
                "algorithm": "md5",
                "value": "90dd1fc82b5d523f6f85716c1c67c0f3"
            })
        ]
    },
    "pid:urn:invalidpath": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [("file_path", "../../file_in_invalid_path")]
    },
    "pid:urn:invalid_file_type_" : {
        "file_characteristics": {
            "file_created": "2018-01-17T08:19:31Z",
            "file_format": "application/unsupported"
        }
    },
    "pid:urn:invalid_file_type_1.0" : {
        "file_characteristics": {
            "file_created": "2018-01-17T08:19:31Z",
            "file_format": "application/unsupported",
            "format_version": "1.0"
        }
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
    },
    "pid:urn:wf_test_1a_ida_missing_file_format": {
        "file_characteristics": {
            "file_created": "2014-01-17T08:19:31Z"
        }
    },
    "pid:urn:1": {
        "set": [
            (
                "file_path", "tests/data/structured/"
                "Access and use rights files/access_file.txt"
            ),
            (
                "parent_directory", {"identifier": "pid:urn:dir:1"}
            ),
            (
                "file_name", "file_name_1"
            )
        ]
    },
    "pid:urn:2": {
        "set": [
            (
                "file_path", "tests/data/structured/"
                "Documentation files/readme.txt"
            ),
            (
                "parent_directory", {"identifier": "pid:urn:dir:2"}
            ),
            (
                "file_name", "file_name_2"
            )
        ]
    },
    "pid:urn:3": {
        "set": [(
            "file_path",
            "tests/data/structured/Documentation files/"
            "Configuration files/properties.txt"
        )]
    },
    "pid:urn:4": {
        "set": [(
            "file_path",
            "tests/data/structured/Documentation files/"
            "Method files/method_putkisto.txt"
        )]
    },
    "pid:urn:5": {
        "set": [(
            "file_path",
            "tests/data/structured/Documentation files/Notebook/notes.txt"
        )]
    },
    "pid:urn:6": {
        "set": [(
            "file_path",
            "tests/data/structured/Documentation files/Other files/this.txt"
        )]
    },
    "pid:urn:7": {
        "set": [(
            "file_path",
            "tests/data/structured/Machine-readable metadata/metadata.txt"
        )]
    },
    "pid:urn:8": {
        "set": [(
            "file_path",
            "tests/data/structured/Publication files/publication.txt"
        )]
    },
    "pid:urn:9": {
        "set": [(
            "file_path",
            "tests/data/structured/Software files/koodi.java"
        )]
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
    },
    "pid:urn:techmd_5": {
        "file_characteristics": HTML_FILE_CHARS,
        "set": [
            ("identifier", "pid:urn:5"),
            ("file_path", "project_x/some/path/file_name_5")
        ]
    },
    "pid:urn:techmd_6": {
        "file_characteristics": XML_FILE_CHARS,
        "set": [
            ("identifier", "pid:urn:6"),
            ("file_path", "project_x/some/path/file_name_6")
        ]
    },
    "pid:urn:techmd_7": {
        "file_characteristics": TXT_FILE_CHARS,
        "set": [
            ("identifier", "pid:urn:7"),
            ("file_path", "project_x/some/path/file_name_7.txt")
        ]
    },
    "pid:urn:techmd_888": {
        "file_characteristics": TIFF_FILE_CHARS,
        "set": [
            ("identifier", "pid:urn:888"),
            ("file_path", "project_x/some/path/valid_tiff.tiff")
        ]
    },
    "pid:urn:techmd_999": {
        "file_characteristics": CSV_FILE_CHARS,
        "set": [
            ("identifier", "pid:urn:999"),
            ("file_path", "project_x/some/path/file.csv")
        ]
    },
    "pid:urn:create_techmd_3": {
        "file_characteristics": {
            "file_created": "2014-01-17T08:19:31Z",
            "file_format": "text/html",
            "format_version": "4.01"
        },
        "set": [("file_path", "/project_x/some/path/file_name_5")]
    },
    "pid:urn:create_techmd_8": {
        "file_characteristics": TIFF_FILE_CHARS,
        "set": [
            (
                "file_path",
                "/project_xml_metadata_missing/some/path/valid_tiff.tiff"
            ),
            ("identifier", "pid:urn:8")
        ]
    },
    "missing_csv_info": {
        "file_characteristics": {
            "file_format": "text/csv"
        }
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
