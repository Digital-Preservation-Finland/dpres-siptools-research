"""Tests for module :mod:`siptools_research.workflow.create_mets`"""
import os

import pytest
import lxml

from siptools.scripts import (
    import_description, premis_event, compile_structmap, import_object
)
from siptools_research.workflow.create_mets import CreateMets
import tests.conftest

NAMESPACES = {
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
    'mets': "http://www.loc.gov/METS/",
    'fi': "http://digitalpreservation.fi/schemas/mets/fi-extensions",
    'premis': "info:lc/xmlns/premis-v2",
    'xlink': "http://www.w3.org/1999/xlink"
}


METS_ATTRIBUTES = {
    'PROFILE': 'http://digitalpreservation.fi/mets-profiles/research-data',
    '{%s}CONTRACTID' % NAMESPACES['fi']:
    "urn:uuid:99ddffff-2f73-46b0-92d1-614409d83001",
    '{%s}schemaLocation' % NAMESPACES['xsi']: 'http://www.loc.gov/METS/ '
                                              'http://digitalpreservation.fi/'
                                              'schemas/mets/mets.xsd',
    '{%s}SPECIFICATION' % NAMESPACES['fi']: '1.7.1',
    'OBJID': 'doi:test',
    '{%s}CATALOG' % NAMESPACES['fi']: '1.7.1',
}


def _check_workspace(testpath):
    """Check that workspace does not contain anything else than mets.xml"""
    files = os.listdir(os.path.join(testpath, 'sip-in-progress'))
    assert len(files) == 1
    assert files[0] == 'mets.xml'


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_mets_ok(testpath):
    """Test the workflow task CreateMets.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create workspace with contents required by the tested task
    create_test_data(workspace=testpath)

    # Init and run task
    task = CreateMets(workspace=testpath, dataset_id='create_mets_dataset',
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()
    _check_workspace(testpath)

    # Read created mets.xml
    tree = lxml.etree.parse(
        os.path.join(testpath, 'sip-in-progress', 'mets.xml')
    )

    # Check that the root element contains expected attributes.
    assert tree.getroot().attrib == METS_ATTRIBUTES

    # Check that XML documents contains expected namespaces
    assert tree.getroot().nsmap == NAMESPACES

    # Check metsHdr element attributes
    metshdr = tree.xpath('/mets:mets/mets:metsHdr', namespaces=NAMESPACES)[0]
    assert metshdr.attrib['RECORDSTATUS'] == 'submission'
    assert metshdr.attrib['CREATEDATE']

    # Check agent element attributes
    archivist = metshdr.xpath("mets:agent[@ROLE='ARCHIVIST']",
                              namespaces=NAMESPACES)[0]
    assert archivist.attrib['TYPE'] == 'ORGANIZATION'
    assert archivist.xpath("mets:name", namespaces=NAMESPACES)[0].text \
        == "Helsingin Yliopisto"
    creator = metshdr.xpath("mets:agent[@ROLE='CREATOR']",
                            namespaces=NAMESPACES)[0]
    assert creator.attrib['ROLE'] == 'CREATOR'
    assert creator.attrib['TYPE'] == 'OTHER'
    assert creator.attrib['OTHERTYPE'] == 'SOFTWARE'
    assert creator.xpath("mets:name", namespaces=NAMESPACES)[0].text \
        == "Packaging Service"


def create_test_data(workspace):
    """Create data needed to run ``CreateMets`` task

    :workspace: Workspace directory in which the data is created.
    """

    # Create directory structure
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Create dmdsec
    import_description.main(['tests/data/datacite_sample.xml',
                             '--workspace', sipdirectory])

    # Create provenance
    premis_event.main(['creation', '2016-10-13T12:30:55',
                       '--workspace', sipdirectory,
                       '--event_outcome', 'success',
                       '--event_detail', 'Poika, 2.985 kg'])

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main(['--workspace', sipdirectory,
                        '--skip_inspection',
                        test_data_folder])

    # Create structmap
    compile_structmap.main(['--workspace', sipdirectory])
