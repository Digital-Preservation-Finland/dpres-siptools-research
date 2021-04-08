"""Module for generating test files at runtime."""
from copy import deepcopy

import lxml.etree

BASE_ADDML_MD = lxml.etree.parse('tests/data/addml_sample.xml')
BASE_AUDIO_MD = lxml.etree.parse('tests/data/audiomd_sample.xml')
BASE_VIDEO_MD = lxml.etree.parse('tests/data/videomd_sample.xml')


PAS_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"

CSV_FILE_METADATA = {
    "file_characteristics": {
        "file_created": "2014-01-17T08:19:31Z",
        "file_format": "text/csv",
        "format_version": "",
        "encoding": "UTF-8",
        "csv_delimiter": ";",
        "csv_record_separator": "CR+LF",
        "csv_quoting_char": "'",
        "csv_has_header": False,
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/csv",
                "stream_type": "text"
            }
        }
    }
}

TXT_FILE_METADATA = {
    "file_characteristics": {
        "encoding": "UTF-8",
        "file_created": "2014-01-17T08:19:31Z",
        "file_format": "text/plain"
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/plain",
                "stream_type": "text"
            }
        }
    }
}

TIFF_FILE_METADATA = {
    "file_characteristics": {
        "file_created": "2018-01-17T08:19:31Z",
        "file_format": "image/tiff",
        "format_version": "6.0",
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "image/tiff",
                "index": 0,
                "bps_unit": "integer",
                "bps_value": "8",
                "byte_order": "little endian",
                "icc_profile_name": "(:unav)",
                "colorspace": "rgb",
                "stream_type": "image",
                "height": "400",
                "width": "640",
                "version": "6.0",
                "samples_per_pixel": "3",
                "compression": "no"
            }
        }
    }
}

HTML_FILE_METADATA = {
    "file_characteristics": {
        "file_format": "text/html",
        "format_version": "5.0",
        "encoding": "UTF-8",
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/html",
                "stream_type": "text"
           }
        }
    }
}

XML_FILE_METADATA = {
    "file_characteristics": {
        "file_format": "text/xml",
        "format_version": "1.0",
        "encoding": "UTF-8",
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/xml",
                "stream_type": "text"
           }
        }
    }
}

MKV_FILE_METADATA = {
    "file_characteristics": {
        "file_format": "video/x-matroska",
        "format_version": "4",
    },
    "file_characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "video/x-matroska",
                "index": 0,
                "stream_type": "videocontainer",
                "version": "4",
                "codec_name": "Matroska",
                "codec_creator_app_version": "57.71.100",
                "codec_creator_app": "Lavf57.71.100"
            },
            1: {
                "mimetype": "video/x-ffv",
                "index": 1,
                "par": "1",
                "frame_rate": "15",
                "data_rate": "(:unav)",
                "bits_per_sample": "8",
                "data_rate_mode": "Variable",
                "color": "Color",
                "codec_quality": "lossless",
                "signal_format": "(:unap)",
                "dar": "1",
                "height": "16",
                "sound": "Yes",
                "version": "0",
                "codec_name": "FFV1",
                "codec_creator_app_version": "57.71.100",
                "duration": "PT1.73S",
                "sampling": "4:2:2",
                "stream_type": "video",
                "width": "16",
                "codec_creator_app": "Lavf57.71.100"
            },
            2: {
                "mimetype": "audio/flac",
                "index": 2,
                "audio_data_encoding": "FLAC",
                "bits_per_sample": "16",
                "data_rate_mode": "Variable",
                "codec_quality": "lossless",
                "version": "1.2.1",
                "stream_type": "audio",
                "sampling_frequency": "48",
                "num_channels": "2",
                "codec_name": "FLAC",
                "codec_creator_app_version": "57.71.100",
                "duration": "PT1.82S",
                "data_rate": "(:unav)",
                "codec_creator_app": "Lavf57.71.100"
            },
            3: {
                "mimetype": "audio/flac",
                "index": 3,
                "audio_data_encoding": "FLAC",
                "bits_per_sample": "16",
                "data_rate_mode": "Variable",
                "codec_quality": "lossless",
                "version": "1.2.1",
                "stream_type": "audio",
                "sampling_frequency": "48",
                "num_channels": "2",
                "codec_name": "FLAC",
                "codec_creator_app_version": "57.71.100",
                "duration": "PT1.82S",
                "data_rate": "(:unav)",
                "codec_creator_app": "Lavf57.71.100"
            }
        }
    }
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
TXT_FILE.update(TXT_FILE_METADATA)

TIFF_FILE = deepcopy(BASE_FILE)
TIFF_FILE.update(TIFF_FILE_METADATA)

CSV_FILE = deepcopy(BASE_FILE)
CSV_FILE.update(CSV_FILE_METADATA)

MKV_FILE = deepcopy(BASE_FILE)
MKV_FILE.update(MKV_FILE_METADATA)


FILES = {
    "pid:urn:textfile1": {
        "file_characteristics": TXT_FILE_METADATA["file_characteristics"],
        "file_characteristics_extension": \
            TXT_FILE_METADATA["file_characteristics_extension"],
        "set": [
            ("file_path", "/path/to/file1"),
            ("checksum", {
                "algorithm": "md5",
                "value": "58284d6cdd8deaffe082d063580a9df3"
            })
        ]
    },
    "pid:urn:textfile2": {
        "file_characteristics": TXT_FILE_METADATA["file_characteristics"],
        "file_characteristics_extension": \
            TXT_FILE_METADATA["file_characteristics_extension"],
        "set": [
            ("file_path", "/path/to/file2"),
            ("checksum", {
                "algorithm": "md5",
                "value": "90dd1fc82b5d523f6f85716c1c67c0f3"
            })
        ]
    },
    "pid:urn:invalid_mimetype_1": TIFF_FILE_METADATA,
    "pid:urn:invalid_mimetype_2": TIFF_FILE_METADATA,
    "pid:urn:not_found_1": TIFF_FILE_METADATA,
    "pid:urn:not_found_2": TIFF_FILE_METADATA,
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

    # Set file_characteristics_extension field
    if "file_characteristics_extension" in FILES[file_id]:
        file_char_ext = FILES[file_id]["file_characteristics_extension"]
        new_file["file_characteristics_extension"] = file_char_ext

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
