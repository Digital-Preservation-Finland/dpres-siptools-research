"""Tests for `siptools_research.utils.scripts.import_objects` module"""
import os
import shutil
import lxml.etree as ET
from siptools_research.utils.scripts import import_objects
from siptools.xml.namespaces import NAMESPACES


def test_import_object_ok(testpath, testmetax):

    # Copy sample file to 'sip-in-progress' directory in workspace
    testbasepath = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(testbasepath)
    testfilepath = os.path.join(testbasepath, 'file_name_5')
    shutil.copy('tests/data/file_name_5', testfilepath)
    testfilepath = os.path.join(testbasepath, 'file_name_6')
    shutil.copy('tests/data/file_name_6', testfilepath)

    return_code = import_objects.main(['3', '--workspace', testpath])
    output_file = os.path.join(testpath, 'file_name_5-techmd.xml')
    tree = ET.parse(output_file)
    root = tree.getroot()

    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text == 'html/text'
    assert root.xpath("//premis:formatVersion", namespaces=NAMESPACES)[0].text == '1.0'
