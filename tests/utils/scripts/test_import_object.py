"""Tests for `siptools_research.utils.scripts.import_objects` module"""
import os
import shutil
import lxml.etree as ET
from siptools_research.utils.scripts import import_objects
from siptools.xml.namespaces import NAMESPACES


def test_import_object_ok(testpath, testmetax):

    # Copy sample files to 'sip-in-progress' directory in workspace
    testbasepath = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(testbasepath)
    metax_filepath = os.path.join(testbasepath, 'project_x_FROZEN/some/path')
    os.makedirs(os.path.join(metax_filepath))
    shutil.copy('tests/data/file_name_5',
                os.path.join(testbasepath, metax_filepath, 'file_name_5'))
    shutil.copy('tests/data/file_name_6',
                os.path.join(testbasepath, metax_filepath, 'file_name_6'))

    # Run import_objects script for a sample dataset
    import_objects.main(['3', '--workspace', testpath])

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(testpath, 'file_name_5-techmd.xml')
    tree = ET.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text == 'html/text'
    assert root.xpath("//premis:formatVersion", namespaces=NAMESPACES)[0].text == '1.0'
