"""Sample Metax file metadata."""
from copy import deepcopy

PAS_STORAGE_SERVICE = "pas"

CSV_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "text/csv",
            "format_version": ""
        },
        "encoding": "UTF-8",
        "csv_delimiter": ";",
        "csv_record_separator": "CRLF",
        "csv_quoting_char": "'",
        "csv_has_header": False,
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/csv",
                "stream_type": "text",
                "first_line": ['a', 'b', '"c"'],
                "charset": "UTF-8",
                "delimiter": ";",
                "separator": "\r\n",
                "quotechar": "'",
           }
        },
        "info": {},
        "mimetype": "text/csv",
        "version": "(:unap)",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:4495863617f91b43dda27ffccc8b3455"
}

TXT_FILE_METADATA = {
    "characteristics": {
        "encoding": "UTF-8",
        "file_format_version": {
            "file_format": "text/plain",
            "format_version": None
        }
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/plain",
                "charset": "UTF-8",
                "stream_type": "text"
            },
        },
        "info": {},
        "mimetype": "text/plain",
        "version": "(:unap)",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:d3b07384d113edec49eaa6238ad5ff00"
}

TIFF_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "image/tiff",
            "format_version": "6.0",
        }
    },
    "characteristics_extension": {
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
        },
        "info": {},
        "mimetype": "image/tiff",
        "version": "6.0",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:3cf7c3b90f5a52b2f817a1c5b3bfbc52"
}

MKV_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "video/x-matroska",
            "format_version": "4"
        }
    },
    "characteristics_extension": {
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
        },
        "info": {},
        "mimetype": "video/x-matroska",
        "version": "4",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:2189cab6a4f7573afc8171381f83e135"
}

AUDIO_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "audio/x-wav",
            "format_version": None
       },
    },
    "characteristics_extension": {
        "streams": {
            "0": {
                'audio_data_encoding': 'PCM',
                'bits_per_sample': '16',
                'codec_creator_app': 'Lavf56.40.101',
                'codec_creator_app_version': '56.40.101',
                'codec_name': 'PCM',
                'codec_quality': 'lossless',
                'data_rate': '768',
                'data_rate_mode': 'Fixed',
                'duration': 'PT0.77S',
                'index': 0,
                'mimetype': 'audio/x-wav',
                'num_channels': '1',
                'sampling_frequency': '48',
                'stream_type': 'audio',
                'version': '(:unap)'
            }
        },
        "info": {},
        "mimetype": "audio/x-wav",
        "version": "(:unap)",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:2b1a74ecc1fa89f182e42bca7719c555"
}

PDF_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "application/pdf",
            "format_version": "A-3b"
        }
    },
    "characteristics_extension": {
        "streams": {
            "0": {
                "index": 0,
                "version": "A-3b",
                "mimetype": "application/pdf",
                "stream_type": "binary"
            }
        },
        "info": {},
        "mimetype": "application/pdf",
        "version": "A-3b",
        "grade": "fi-dpres-recommended-file-format"
    },
    "checksum": "md5:5db57524e33bbf53c13d256234b92fbd"
}

VIDEO_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "video/dv",
            "format_version": None
        }
    },
    "characteristics_extension": {
        "streams": {
            "0": {
                "dar": "1.778",
                "par": "1.422",
                "color": "Color",
                "index": 0,
                "sound": "No",
                "width": "720",
                "height": "576",
                "version": "(:unap)",
                "duration": "PT0.08S",
                "mimetype": "video/dv",
                "sampling": "4:2:0",
                "data_rate": "24.4416",
                "codec_name": "DV",
                "frame_rate": "25",
                "stream_type": "video",
                "codec_quality": "lossy",
                "signal_format": "PAL",
                "data_rate_mode": "Fixed",
                "bits_per_sample": "8",
                "codec_creator_app": "(:unav)",
                "codec_creator_app_version": "(:unav)"
            }
        },
        "info": {},
        "mimetype": "video/dv",
        "version": "(:unap)",
        "grade": "fi-dpres-acceptable-file-format"
    },
    "checksum": "md5:646912efe14a049ceb9f3a6f741d7b66"
}

SEG_Y_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "application/x.fi-dpres.segy",
            "format_version": "1.0"
        }
    },
    "characteristics_extension": {
        "streams": {
            "0": {
                "index": 0,
                "version": "1.0",
                "mimetype": "application/x.fi-dpres.segy",
                "stream_type": "binary"
            }
        },
        "info": {},
        "mimetype": "application/x.fi-dpres.segy",
        "version": "1.0",
        "grade": "fi-dpres-bit-level-file-format"
    },
    "checksum": "md5:c5edc06ea17769fd38c5f6014f29b5f3"
}

BASE_CHARACTERISTICS = {
    "file_format_version": None,
    "encoding": None,
    "csv_delimiter": None,
    "csv_record_separator": None,
    "csv_quoting_char": None,
    "csv_has_header": None,
}
BASE_FILE = {
    "id": "pid:urn:identifier",
    "pathname": "/path/to/file",
    "filename": "file",
    "size": 100,
    "storage_service": "ida",
    "checksum": "md5:58284d6cdd8deaffe082d063580a9df3",
    "csc_project": "test_project",
    "characteristics": BASE_CHARACTERISTICS,
    "characteristics_extension": None,
}

TXT_FILE = deepcopy(BASE_FILE)
TXT_FILE.update(TXT_FILE_METADATA)
TXT_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
TXT_CHARACTERISTICS.update(TXT_FILE["characteristics"])
TXT_FILE["characteristics"] = TXT_CHARACTERISTICS

TIFF_FILE = deepcopy(BASE_FILE)
TIFF_FILE.update(TIFF_FILE_METADATA)
TIFF_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
TIFF_CHARACTERISTICS.update(TIFF_FILE["characteristics"])
TIFF_FILE["characteristics"] = TIFF_CHARACTERISTICS
TIFF_FILE["id"] += "_tiff"
TIFF_FILE["pathname"] += ".tiff"

CSV_FILE = deepcopy(BASE_FILE)
CSV_FILE.update(CSV_FILE_METADATA)
CSV_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
CSV_CHARACTERISTICS.update(CSV_FILE["characteristics"])
CSV_FILE["characteristics"] = CSV_CHARACTERISTICS
CSV_FILE["id"] += "_csv"
CSV_FILE["pathname"] += ".csv"

MKV_FILE = deepcopy(BASE_FILE)
MKV_FILE.update(MKV_FILE_METADATA)
MKV_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
MKV_CHARACTERISTICS.update(MKV_FILE["characteristics"])
MKV_FILE["characteristics"] = MKV_CHARACTERISTICS
MKV_FILE["id"] += "_mkv"
MKV_FILE["pathname"] += ".mkv"

AUDIO_FILE = deepcopy(BASE_FILE)
AUDIO_FILE.update(AUDIO_FILE_METADATA)
AUDIO_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
AUDIO_CHARACTERISTICS.update(AUDIO_FILE["characteristics"])
AUDIO_FILE["characteristics"] = AUDIO_CHARACTERISTICS
AUDIO_FILE["id"] += "_wav"
AUDIO_FILE["pathname"] += ".wav"

PDF_FILE = deepcopy(BASE_FILE)
PDF_FILE.update(PDF_FILE_METADATA)
PDF_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
PDF_CHARACTERISTICS.update(PDF_FILE["characteristics"])
PDF_FILE["characteristics"] = PDF_CHARACTERISTICS
PDF_FILE["id"] += "_pdf"
PDF_FILE["pathname"] += ".pdf"

VIDEO_FILE = deepcopy(BASE_FILE)
VIDEO_FILE.update(VIDEO_FILE_METADATA)
VIDEO_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
VIDEO_CHARACTERISTICS.update(VIDEO_FILE["characteristics"])
VIDEO_FILE["characteristics"] = VIDEO_CHARACTERISTICS
VIDEO_FILE["id"] += "_dv"
VIDEO_FILE["pathname"] += ".dv"

SEG_Y_FILE = deepcopy(BASE_FILE)
SEG_Y_FILE.update(SEG_Y_FILE_METADATA)
SEG_Y_CHARACTERISTICS = deepcopy(BASE_CHARACTERISTICS)
SEG_Y_CHARACTERISTICS.update(SEG_Y_FILE["characteristics"])
SEG_Y_FILE["characteristics"] = SEG_Y_CHARACTERISTICS
SEG_Y_FILE["id"] += ".sgy"
SEG_Y_FILE["pathname"] += ".sgy"
