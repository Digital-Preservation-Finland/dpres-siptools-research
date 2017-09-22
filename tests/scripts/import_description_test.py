# coding=utf8
from tempfile import NamedTemporaryFile
import lxml.etree as ET
import siptools.xml.ead3 as ead3
from siptools.xml.namespaces import NAMESPACES
from siptools.scripts.import_description import main
import pytest
import os
from urllib import quote_plus

EAD3_NS = 'http://ead3.archivists.org/schema/'
METS_NS = 'http://www.loc.gov/METS/'
dmdsec_location = 'tests/data/import_description/metadata/sahke2_test-ead3.xml'


def test_import_description_valid_file(testpath):
    """ Test case for single valid xml-file"""

    main([dmdsec_location, '--workspace', testpath])

def ead3_ns(tag):

    path = '{%s}%s' % (EAD3_NS, tag)

    return path

def mets_ns(tag):

    path = '{%s}%s' % (METS_NS, tag)

    return path

def test_ead3_ok(testpath):

    test_import_description_valid_file(testpath)

    output_file = os.path.join(testpath, 'dmdsec.xml')

    tree = ET.parse(output_file)
    root = tree.getroot()

    print root.xpath("//ead3:ead/*[1]",
                      namespaces=NAMESPACES)[0].tag

    assert root.xpath("/mets:mets/*[1]",
                      namespaces=NAMESPACES)[0].tag == mets_ns('dmdSec')

    assert root.xpath("/mets:mets/*[1]/*[1]",
                      namespaces=NAMESPACES)[0].tag == mets_ns('mdWrap')

    assert len(root.xpath("/mets:mets/*[1]/*[1]",
                      namespaces=NAMESPACES)) == 1

    assert root.xpath("/mets:mets/*[1]/*[1]/@MDTYPE",
            namespaces=NAMESPACES)[0] == 'OTHER'

    assert root.xpath("/mets:mets/*[1]/*[1]/*[1]/*[1]",
                      namespaces=NAMESPACES)[0].tag == ead3_ns('ead')

    assert len(root.xpath("/mets:mets/*[1]/*[1]/*[1]/*[1]",
                      namespaces=NAMESPACES)) == 1

    assert root.xpath("//ead3:ead/@relatedencoding",
                      namespaces=NAMESPACES)[0] == u'SÄHKE2'

    assert root.xpath("//ead3:ead/*[2]",
                      namespaces=NAMESPACES)[0].tag == ead3_ns('archdesc')

    assert root.xpath("//ead3:ead/ead3:control/*[1]",
                      namespaces=NAMESPACES)[0].tag == ead3_ns('recordid')

    assert root.xpath("//ead3:ead/ead3:control/ead3:filedesc/*[1]/*[1]",
                      namespaces=NAMESPACES)[0].tag == ead3_ns('titleproper')

    assert root.xpath("count(//ead3:ead/ead3:archdesc/ead3:did)",
            namespaces=NAMESPACES) == 1

