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
        "csv_record_separator": "CR+LF",
        "csv_quoting_char": "'",
        "csv_has_header": False,
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/csv",
                "stream_type": "text"
            }
        }
    }
}

TXT_FILE_METADATA = {
    "characteristics": {
        "encoding": "UTF-8",
        "file_format_version": {
            "file_format": "text/plain"
        }
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/plain",
                "stream_type": "text"
            }
        }
    }
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
        }
    }
}

HTML_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "text/html",
            "format_version": "5.0"
        },
        "encoding": "UTF-8",
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/html",
                "stream_type": "text"
            }
        }
    }
}

XML_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "text/xml",
            "format_version": "1.0"
        },
        "encoding": "UTF-8",
    },
    "characteristics_extension": {
        "streams": {
            0: {
                "mimetype": "text/xml",
                "stream_type": "text"
            }
        }
    }
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
        }
    }
}

AUDIO_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "audio/x-wav"
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
        }
    }
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
        }
    }
}

VIDEO_FILE_METADATA = {
    "characteristics": {
        "file_format_version": {
            "file_format": "video/dv"
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
        }
    }
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
        }
    }
}

BASE_FILE = {
    "id": "pid:urn:identifier",
    "pathname": "/path/to/file",
    "storage_service": "ida",
    # "parent_directory": {
    #    "identifier": "pid:urn:dir:wf1"
    # },
    "checksum": "md5:58284d6cdd8deaffe082d063580a9df3",
    "csc_project": "test_project"
}

TXT_FILE = deepcopy(BASE_FILE)
TXT_FILE.update(TXT_FILE_METADATA)

TIFF_FILE = deepcopy(BASE_FILE)
TIFF_FILE.update(TIFF_FILE_METADATA)
TIFF_FILE["id"] += "_tiff"
TIFF_FILE["pathname"] += ".tiff"

CSV_FILE = deepcopy(BASE_FILE)
CSV_FILE.update(CSV_FILE_METADATA)
CSV_FILE["id"] += "_csv"
CSV_FILE["pathname"] += ".csv"

MKV_FILE = deepcopy(BASE_FILE)
MKV_FILE.update(MKV_FILE_METADATA)
MKV_FILE["id"] += "_mkv"
MKV_FILE["pathname"] += ".mkv"

AUDIO_FILE = deepcopy(BASE_FILE)
AUDIO_FILE.update(AUDIO_FILE_METADATA)
AUDIO_FILE["id"] += "_wav"
AUDIO_FILE["pathname"] += ".wav"

PDF_FILE = deepcopy(BASE_FILE)
PDF_FILE.update(PDF_FILE_METADATA)
PDF_FILE["id"] += "_pdf"
PDF_FILE["pathname"] += ".pdf"

VIDEO_FILE = deepcopy(BASE_FILE)
VIDEO_FILE.update(VIDEO_FILE_METADATA)
VIDEO_FILE["id"] += "_dv"
VIDEO_FILE["pathname"] += ".dv"

SEG_Y_FILE = deepcopy(BASE_FILE)
SEG_Y_FILE.update(SEG_Y_FILE_METADATA)
SEG_Y_FILE["id"] += ".sgy"
SEG_Y_FILE["pathname"] += ".sgy"
