from tempfile import NamedTemporaryFile
import lxml.etree as ET
from siptools_research.scripts import import_objects_sahke2
from siptools.xml.namespaces import NAMESPACES
import pytest
import os
from urllib import quote_plus


def test_import_object_ok(testpath):

    return_code = import_objects_sahke2.main(['tests/data/import_description/metadata',
            '--sahke2', 'sahke2_test.xml', '--workspace', testpath])
    output_file = os.path.join(testpath, 'sahke_xml%2Ftest.xml-techmd.xml')
    tree = ET.parse(output_file)
    root = tree.getroot()

    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text == 'text/xml; charset=utf-8'
    assert root.xpath("//premis:formatVersion", namespaces=NAMESPACES)[0].text == '1.0'
    assert root.xpath("//premis:messageDigest", namespaces=NAMESPACES)[0].text == '111111111'

    assert return_code == 0

