from tempfile import NamedTemporaryFile
import lxml.etree as ET
from siptools_research.utils.scripts import import_objects
from siptools.xml.namespaces import NAMESPACES
import pytest
import os
import shutil
from urllib import quote_plus


def test_import_object_ok(testpath):

    # Create 'files' directory with testfile in workspace
    testbasepath = os.path.join(testpath, 'files','some', 'path')
    os.makedirs(testbasepath)
    testfilepath = os.path.join(testbasepath, 'file_name_11')
    shutil.copy('tests/data/file_name_11', testfilepath)

    return_code = import_objects.main(['11', '--workspace', testpath])
    output_file = os.path.join(testpath, 'file_name_11-techmd.xml')
    tree = ET.parse(output_file)
    root = tree.getroot()

    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text == 'html/text'
    assert root.xpath("//premis:formatVersion", namespaces=NAMESPACES)[0].text == '1.0'

    assert return_code == 0

