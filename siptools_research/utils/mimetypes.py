'''
Created on 22 Mar 2018

@author: vagrant
'''
import json

DEFAULT_CONFIG = '/etc/dpres_mimetypes.json'


def is_supported(input_file_format, output_format_version):
    for mime_type in _get_supported_mimetypes():
        if mime_type["input_file_format"] == input_file_format \
         and mime_type["output_format_version"] == output_format_version:
            return True
    return False


def _check_consistency(file_path=DEFAULT_CONFIG):
    for mime_type in _get_supported_mimetypes(file_path):
        ffv = "file_format_version_" + \
            mime_type["input_file_format"].replace('/', '_')
        uri = "http://purl.org/att/es/reference_data/file_format_version/"
        uri += "file_format_version_" + \
            mime_type["input_file_format"].replace('/', '_')
        if mime_type["output_format_version"] != "":
            assert mime_type["id"] == ffv + "_" + \
                mime_type["output_format_version"].replace('/', '_'), \
                "Inconsistency in: id=" + mime_type["id"]
            assert mime_type["uri"] == uri + "_" + \
                mime_type["output_format_version"].replace('/', '_'), \
                "Inconsistency in: id=" + mime_type["id"]
        else:
            assert mime_type["id"] == ffv, \
                "Inconsistency in: id=" + mime_type["id"]
            assert mime_type["uri"] == uri, \
                "Inconsistency in: id=" + mime_type["id"]
    return True


def _get_supported_mimetypes(file_path=DEFAULT_CONFIG):
    with open(_get_mimetypes_filepath(file_path)) as json_data_file:
        return json.load(json_data_file)


def _get_mimetypes_filepath(file_path=DEFAULT_CONFIG):
    return file_path