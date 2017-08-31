from tempfile import NamedTemporaryFile
import lxml.etree as ET
from siptools.scripts import compile_mets
from siptools.scripts import import_description
from siptools_research.scripts import create_digiprov_sahke2
from siptools_research.scripts import import_objects_sahke2
from siptools_research.scripts import compile_ead3_structmap
from siptools.xml.namespaces import NAMESPACES
import pytest
import os
from urllib import quote_plus


def create_test_data(testpath):

    # create descriptive metadata
    import_description.main([
            'tests/data/import_description/metadata/sahke2_test-ead3.xml',
            '--workspace', testpath])

    # create provencance metadata
    create_digiprov_sahke2.main([
            'tests/data/import_description/metadata/sahke2_test-ead3.xml',
            '--workspace', testpath])

    # create technical metadata
    import_objects_sahke2.main(['tests/data/import_description/metadata',
            '--sahke2', 'sahke2_test.xml', '--workspace', testpath])

    # create structural metadata
    compile_ead3_structmap.main(['tests/data/import_description/metadata',
            'tests/data/import_description/metadata/sahke2_test-ead3.xml',
            'sahke2_test.xml', '--workspace', testpath, '--clean'])

def test_compile_mets_ok(testpath):

    create_test_data(testpath)

    return_code = compile_mets.main(['--workspace', testpath,
            'kdk', 'Kansallisarkisto', '--clean'])

    mets = os.path.join(testpath, 'mets.xml')
    tree = ET.parse(mets)
    root = tree.getroot()

    assert len(root.xpath(
            "/mets:mets[@PROFILE='http://www.kdk.fi/kdk-mets-profile']",
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath(
            "//mets:metsHdr[@RECORDSTATUS='submission']",
            namespaces=NAMESPACES)) == 1
    assert root.xpath(
            "/mets:mets/mets:metsHdr/mets:agent/mets:name",
            namespaces=NAMESPACES)[0].text == 'Kansallisarkisto'
    assert len(root.xpath('/mets:mets/mets:metsHdr',
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath('/mets:mets/mets:dmdSec',
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath('/mets:mets/mets:amdSec',
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath('/mets:mets/mets:fileSec',
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath('/mets:mets/mets:structMap',
            namespaces=NAMESPACES)) == 1
    assert len(root.xpath('/mets:mets/mets:structMap/mets:div/*',
            namespaces=NAMESPACES)) == 2
    assert root.xpath('/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
            namespaces=NAMESPACES)[0] == 'archdesc'
    assert root.xpath('/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
            namespaces=NAMESPACES)[1] == 'SAHKE2-file'

    assert return_code == 0

