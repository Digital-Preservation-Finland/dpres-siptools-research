"""Check file format support"""
import json

import pkg_resources


def is_supported(mimetype, version):
    """Check if file format is supported. Supported file formats are listed in
    JSON configuration file.

    :param mimetype: file mimetype
    :param version: file format version
    :returns: ``True`` or ``False``
    """
    data = pkg_resources.resource_string(
        "siptools_research", "data/dpres_mimetypes.json"
    )
    supported_file_formats = json.loads(data)

    for file_format in supported_file_formats:
        if file_format["input_file_format"] == mimetype\
                and file_format["output_format_version"] == version:
            return True

    return False
