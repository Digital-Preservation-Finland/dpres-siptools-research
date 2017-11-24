# coding=utf-8
"""Creates PREMIS file metadata for objects by reading METAX data
and using siptools for creation of data.
"""

import os
import sys
import argparse
import urllib

import lxml.etree

from siptools.scripts import import_object
from siptools.xml.namespaces import NAMESPACES
from siptools_research.utils.metax import Metax


def parse_arguments(arguments):
    """ Create arguments parser and return parsed command line
    arguments.
    """
    parser = argparse.ArgumentParser(description='Tool for '
                                     'creating premis events')
    parser.add_argument("dataset_id", type=str, help="Metax id of dataset")
    parser.add_argument('--workspace', dest='workspace', type=str,
                        default='./workspace', help="Workspace directory")

    return parser.parse_args(arguments)


def create_premis_object(digital_object, filepath, formatname, creation_date,
                         hashalgorithm, hashvalue, format_version, workspace):
    """Calls import_object from siptools to create
    PREMIS file metadata.
    """
    import_object.main([digital_object, '--base_path', filepath,
                        '--workspace', workspace, '--skip_inspection',
                        '--format_name', formatname,
                        '--digest_algorithm', hashalgorithm,
                        '--message_digest', hashvalue,
                        '--date_created', creation_date,
                        '--format_version', format_version])


def create_objects(file_id=None, metax_filepath=None, workspace=None):
    """Gets file metadata from Metax and calls create_premis_object function"""

    metadata = Metax().get_data('files', file_id)

    filename = metadata["file_name"]
    # Assume that files are found in 'sip-in-progress' directory in workspace
    filepath = os.path.join(workspace, 'sip-in-progress')
    hashalgorithm = metadata["checksum"]["algorithm"]
    hashvalue = metadata["checksum"]["value"]
    creation_date = metadata["file_characteristics"]["file_created"]
    formatname = metadata["file_format"]
    # formatversion hardcoded. Not in METAX yet. could be retrieved from file:
    #    formatname = formatdesignation(filepath, datatype='name')
    #    formatversion = formatdesignation(filepath, datatype='version')
    formatversion = "1.0"

    # Picks name of hashalgorithm from its length if it's not valid
    allowed_hashs = {128: 'MD5', 160: 'SHA-1', 224: 'SHA-224',
                     256: 'SHA-256', 384: 'SHA-384', 512: 'SHA-512'}
    hash_bit_length = len(hashvalue) * 4

    if hashalgorithm in allowed_hashs.values():
        hashalgorithm = hashalgorithm
    elif hash_bit_length in allowed_hashs:
        hashalgorithm = allowed_hashs[hash_bit_length]
    else:
        hashalgorithm = 'ERROR'

    create_premis_object(filename, filepath, formatname,
                         creation_date, hashalgorithm,
                         hashvalue, formatversion,
                         workspace)

    # write xml files if they exist
    xml = Metax().get_xml('files', file_id)
    for ns_url in xml:
        if ns_url not in NAMESPACES.values():
            raise TypeError("Invalid XML namespace: %s" % ns_url)
        xml_data = xml[ns_url]
        ns_key = next((key for key, url in NAMESPACES.items() if url\
                       == ns_url), None)
        target_filename = urllib.quote_plus(metax_filepath + '-' + ns_key\
                                            + '-techmd.xml')
        output_file = os.path.join(workspace, target_filename)
        with open(output_file, 'w+') as outfile:
            outfile.write(lxml.etree.tostring(xml_data))

    return 0


def main(arguments=None):
    """The main method for argparser"""
    args = parse_arguments(arguments)

    metax_dataset = Metax().get_data('datasets', args.dataset_id)
    for file_section in metax_dataset["research_dataset"]["files"]:
        file_id = file_section["identifier"]
        metax_filepath = file_section['type']['pref_label']['en'].strip('/')
        create_objects(file_id, metax_filepath, args.workspace)

    return 0


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)
