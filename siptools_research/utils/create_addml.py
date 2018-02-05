# coding=utf-8
"""Utils for reading and writing ADDML data.
"""

import os
import sys

import lxml.etree as ET
import mets as m
import xml_helpers.utils as h
import addml as a

from siptools.utils import encode_path, encode_id
from siptools_research.utils import utils

def create_techmdfile(sip_creation_path, mdtype, mdtypeversion, mddata, filename):
    """Writes a METS techMD file and appends the mdtype data to xmlData.
    Returns the name of the created file.
    """
    prefix = '%s-' % mdtype
    filename = encode_path(filename, prefix=prefix, suffix="-othermd.xml")
    fileid = encode_id(filename)
    mets = m.mets()
    amdsec = m.amdsec()
    techmd = m.techmd(fileid)
    mdwrap = m.mdwrap('OTHER', mdtypeversion, othermdtype=mdtype)
    xmldata = m.xmldata()

    xmldata.append(mddata)
    mdwrap.append(xmldata)
    techmd.append(mdwrap)
    amdsec.append(techmd)
    mets.append(amdsec)

    with open(os.path.join(sip_creation_path, filename), 'w+') as outfile:
        outfile.write(h.serialize(mets))
        print "Wrote METS %s technical metadata to file %s" % (mdtype,
                outfile.name)

    return fileid


def add_to_tempfile(root, othermdfile, sourcefilepath, mdtype, mdfile_loc):
    """Adds the name of the metadatafile and the source file to be
    paired in METS fileSec.
    """
    prefix = '%s-' % mdtype
    fileid = ET.Element('fileid')
    root.append(fileid)
    filename = encode_path(othermdfile, prefix=prefix, suffix="-othermd.xml")
    if mdfile_loc:
        sourcefilepath = os.path.join(mdfile_loc, sourcefilepath)
    fileid.text = encode_id(filename)
    fileid.set('path', sourcefilepath)

    return root


def csv_header(csv_file_path, delimiter, isheader=False, headername='header'):
    """Returns header of CSV file if there is one. 
    Otherwise generaters a header and returns it"""
    csv_file = open(csv_file_path, 'r')
    header = csv_file.readline()
    if not isheader:
        header_count = header.count(delimiter) 
        header = headername + str(1)
        for i in range(0, header_count):
            header = header + delimiter + headername + str(i + 2)
    return header


def create_addml(sip_creation_path, filename, delimiter, isheader, charset, recordSeparator, quotingChar):
    """Creates ADDML metadatafor a csv file.
    Returns created ADDML metadata.
    """

    header = csv_header(os.path.join(sip_creation_path, filename), delimiter, isheader)

    description = ET.Element(a.addml_ns('description'))
    reference = ET.Element(a.addml_ns('reference'))
    flatfile = a.definition_elems('flatFile', filename, 'testdef')

    headers = header.split(delimiter)
    fieldDefinitions = a.wrapper_elems('fieldDefinitions')
    for col in headers:
        fieldDefinitions.append(a.definition_elems('fieldDefinition', col,
            'String'))

    recordDefinition = a.definition_elems('recordDefinition', 'testrecord',
            'testrectype', [fieldDefinitions])
    recordDefinitions = a.wrapper_elems('recordDefinitions', [recordDefinition])
    flatFileDefinition = a.definition_elems('flatFileDefinition', 'testdef',
            'testtype', [recordDefinitions])
    flatFileDefinitions = a.wrapper_elems('flatFileDefinitions',
            [flatFileDefinition])

    dataType = a.addml_basic_elem('dataType', 'string')
    fieldType = a.definition_elems('fieldType', 'String', child_elements=[dataType])
    fieldTypes = a.wrapper_elems('fieldTypes', [fieldType])

    trimmed = ET.Element(a.addml_ns('trimmed'))
    recordType = a.definition_elems('recordType', 'testrectype',
            child_elements=[trimmed])
    recordTypes = a.wrapper_elems('recordTypes', [recordType])

    delimFileFormat = a.delimfileformat(recordSeparator, delimiter, quotingChar)
    charset_elem = a.addml_basic_elem('charset', charset)
    flatFileType = a.definition_elems('flatFileType', 'testtype',
            child_elements=[charset_elem, delimFileFormat])
    flatFileTypes = a.wrapper_elems('flatFileTypes', [flatFileType])

    structureTypes = a.wrapper_elems('structureTypes', [flatFileTypes, recordTypes, fieldTypes])
    flatfiles = a.wrapper_elems('flatFiles', [flatfile, flatFileDefinitions,
        structureTypes])

    addml_root = a.addml([description, reference, flatfiles])

    return addml_root


def main(arguments=None):
    sip_creation_path = "./workspace"
    csv_filename = "csvfile.csv"
    csv_file_path = os.path.join ('files/', csv_filename)
    delimiter = ";"
    charset = "UTF-8"
    recordSeparator = "CR+LF"
    quotingChar = '"'
    isheader = False
    mdtype = 'ADDML'
    mdtypeversion = '8.3'

    utils.makedirs_exist_ok(sip_creation_path)

    addml = create_addml(sip_creation_path, csv_file_path, delimiter, isheader, charset, recordSeparator,
            quotingChar)
    file_id = create_techmdfile(sip_creation_path, mdtype, mdtypeversion,
            addml, csv_file_path)
    print h.serialize(addml)
    print "file_id:%s" % file_id


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)

