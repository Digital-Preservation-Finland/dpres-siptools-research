"""Check file format support"""
import json


def is_supported(mimetype, version, config):
    """Check if file format is supported. Supported file formats are listed in
    JSON configuration file.

    :param mimetype: file mimetype
    :param version: file format version
    :param config: path to configuration file
    :returns: ``True`` or ``False``
    """
    with open(config) as config_file:
        supported_file_formats = json.load(config_file)

    for file_format in supported_file_formats:
        if file_format["input_file_format"] == mimetype\
                and file_format["output_format_version"] == version:
            return True

    return False
