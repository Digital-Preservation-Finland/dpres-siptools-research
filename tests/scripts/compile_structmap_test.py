from tempfile import NamedTemporaryFile
import lxml.etree as ET
from siptools_research.scripts import compile_ead3_structmap
from siptools_research.scripts import import_objects_sahke2
from siptools.xml.namespaces import NAMESPACES
import pytest
import os
from urllib import quote_plus


def create_test_data(testpath):

    import_objects_sahke2.main(['tests/data/import_description/metadata',
            '--sahke2', 'sahke2_test.xml', '--workspace', testpath])

def test_compile_structmap_ok(testpath):

    create_test_data(testpath)
    return_code = compile_ead3_structmap.main(['tests/data/import_description/metadata',
            'tests/data/import_description/metadata/sahke2_test-ead3.xml',
            'sahke2_test.xml', '--workspace', testpath, '--clean'])

    filesec = os.path.join(testpath, 'filesec.xml')
    fs_tree = ET.parse(filesec)
    fs_root = fs_tree.getroot()

    structmap = os.path.join(testpath, 'structmap.xml')
    sm_tree = ET.parse(structmap)
    sm_root = sm_tree.getroot()

    assert len(fs_root.xpath('//mets:fileSec', namespaces=NAMESPACES)) == 1
    assert len(sm_root.xpath('//mets:structMap', namespaces=NAMESPACES)) == 1
    assert len(sm_root.xpath('//mets:structMap/mets:div/*',
            namespaces=NAMESPACES)) == 2
    assert sm_root.xpath('//mets:structMap/mets:div/mets:div/@TYPE',
            namespaces=NAMESPACES)[0] == 'archdesc'
    assert sm_root.xpath('//mets:structMap/mets:div/mets:div/@TYPE',
            namespaces=NAMESPACES)[1] == 'SAHKE2-file'

    assert return_code == 0

