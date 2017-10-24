# coding=utf-8
"""Creates PREMIS file metadata for objects by reading METAX data
and using siptools for creation of data.
"""

import os
import sys
import argparse
import subprocess

import lxml.etree as ET
from wand.image import Image

from siptools.scripts import import_object
from siptools.utils import encode_path
from siptools.xml.namespaces import NAMESPACES
import siptools.xml.mets as m
import siptools.xml.premis as p
import siptools.xml.mix as mix
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
    #  For some reason the "files"-argument has to be a directory that is found in
    #  base_path, not a file. Therefore "files" is set to "./".
    import_object.main(['./', '--base_path', filepath,
                        '--workspace', workspace, '--skip_inspection',
                        '--format_name', formatname,
                        '--digest_algorithm', hashalgorithm,
                        '--message_digest', hashvalue,
                        '--date_created', creation_date,
                        '--format_version', format_version])


def create_objects(file_id=None, workspace=None):
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
    #print "filename:%s, filepath:%s, hashalgorithm:%s, hashvalue:%s, \
    #creation_date:%s, formatname:%s" % \
    #(filename,filepath,hashalgorithm,hashvalue,creation_date,formatname)

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

    """
    if formatdesignation(filepath, datatype='mimemaintype') == 'image':

        techmd_file = encode_path(filepath, suffix='-techmd.xml')
        techmd_path = os.path.join('./workspace', techmd_file)

        tree = ET.parse(techmd_path)
        root = tree.getroot()

        obj_c = root.xpath("//premis:objectCharacteristics",
                           namespaces=NAMESPACES)[0]

        obj_ext = ET.SubElement(obj_c,
                                p.premis_ns(
                                    'objectCharacteristicsExtension'))

        create_mix = write_mix(filepath)

        mix_data = ET.fromstring(create_mix)

        obj_ext.append(mix_data)

        with open(techmd_path, 'w+') as outfile:
            outfile.write(m.serialize(root))


    s2_creation_date = import_object.creation_date(s2_path)

    s2_md5 = import_object.md5(s2_path)

    s2_formatname = formatdesignation(s2_path, datatype='name')
    s2_formatversion = formatdesignation(s2_path, datatype='version')

    create_premis_object(s2_name, package_path, s2_formatname,
                         s2_creation_date, 'MD5', s2_md5,
                         s2_formatversion, workspace)

    """

    return 0


def formatdesignation(filepath, datatype=None):
    """Returns information about the file format, it's name and version,
    using the unix file command. Different processing intstructions for
    different files.
    """
    command_bi = ['file', '-bi', filepath]
    command_i = ['file', '-b', filepath]
    formatinfo = subprocess.Popen(
        command_i, stdout=subprocess.PIPE).stdout.read()
    formats = formatinfo.split(' ')

    # If it's an XML-file, return fixed mimetype, version and charset
    if formats[0] == 'XML' and formats[1] == '1.0':
        charset = str(subprocess.Popen(
            command_bi,
            stdout=subprocess.PIPE).stdout.read()).rsplit(None, 1)[-1]
        formatname = 'text/xml; ' + charset
        formatversion = '1.0'

    # If it's a PDF-file, return mimetype and version
    elif formats[0] == 'PDF':
        formatname = subprocess.Popen(
            command_bi,
            stdout=subprocess.PIPE).stdout.read().rsplit(';', 1)[0]
        formatversion = subprocess.Popen(
            command_i,
            stdout=subprocess.PIPE).stdout.read().rsplit(None, 1)[-1]

    # If it's a TIFF-file, return mimetype and fixed version
    elif formats[0] == 'TIFF':
        formatname = subprocess.Popen(
            command_bi,
            stdout=subprocess.PIPE).stdout.read().rsplit(';', 1)[0]
        formatversion = '6.0'

    # If it's an Open Office document return mimetype and fixed version
    elif formats[0] == 'OpenDocument':
        formatname = subprocess.Popen(
            command_bi,
            stdout=subprocess.PIPE).stdout.read().rsplit(';', 1)[0]
        formatversion = '1.0'

    elif formats[0] == 'JPEG':
        formatname = subprocess.Popen(
            command_bi,
            stdout=subprocess.PIPE).stdout.read().rsplit(';', 1)[0]
        formatversion = '6.0'

    mimemaintype = subprocess.Popen(
        command_bi,
        stdout=subprocess.PIPE).stdout.read().rsplit('/', 1)[0]

    if datatype == 'name':
        return formatname
    elif datatype == 'version':
        return formatversion
    elif datatype == 'mimemaintype':
        return mimemaintype


def write_mix(img):
    """Write MIX technical metadata if it's an image file."""
    with Image(filename=img) as i:
        byteorder = None
        width = str(i.width)
        height = str(i.height)
        colorspace = str(i.colorspace)
        bitspersample = str(i.depth)
        bpsunit = 'integer'
        # itype = str(i.type)
        compression = str(i.compression)
        if colorspace.startswith('gray'):
            samplesperpixel = '1'
        else:
            samplesperpixel = '3'
        metadata = i.metadata.items()
        for key, value in metadata:
            if key.startswith('tiff:endian'):
                if value == 'msb':
                    byteorder = 'big endian'
                elif value == 'lsb':
                    byteorder = 'little endian'

    mix_compression = mix.mix_Compression(compressionScheme=compression)

    basicdigitalobjectinformation = mix.mix_BasicDigitalObjectInformation(
        byteOrder=byteorder, Compression_elements=[mix_compression])

    basicimageinformation = mix.mix_BasicImageInformation(
        imageWidth=width,
        imageHeight=height,
        colorSpace=colorspace)

    imageassessmentmetadata = mix.mix_ImageAssessmentMetadata(
        bitsPerSampleValue_elements=bitspersample,
        bitsPerSampleUnit=bpsunit, samplesPerPixel=samplesperpixel)

    mix_root = mix.mix_mix(
        BasicDigitalObjectInformation=basicdigitalobjectinformation,
        BasicImageInformation=basicimageinformation,
        ImageAssessmentMetadata=imageassessmentmetadata)

    create_mix = mix.serialize(mix_root)

    return create_mix


def main(arguments=None):
    """The main method for argparser"""
    args = parse_arguments(arguments)

    metax_dataset = Metax().get_data('datasets', args.dataset_id)
    for i in metax_dataset["research_dataset"]["files"]:
        file_id = i["identifier"]
        create_objects(file_id, args.workspace)


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)
