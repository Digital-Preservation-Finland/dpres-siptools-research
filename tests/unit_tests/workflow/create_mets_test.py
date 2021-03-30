"""Tests for module :mod:`siptools_research.workflow.create_mets`."""
import os

import pytest
import lxml

from siptools.scripts.import_description import import_description
from siptools.scripts.premis_event import premis_event
from siptools.scripts.compile_structmap import compile_structmap
from siptools.scripts.import_object import import_object

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
    '{%s}SPECIFICATION' % NAMESPACES['fi']: '1.7.3',
    'OBJID': 'doi:test',
    '{%s}CATALOG' % NAMESPACES['fi']: '1.7.3',
}


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_create_mets_ok(testpath, requests_mock):
    """Test the workflow task CreateMets.

    :param testpath: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get(
        "https://metaksi/rest/v1/contracts/"
        "urn:uuid:99ddffff-2f73-46b0-92d1-614409d83001",
        json={
            'contract_json':
            {
                'identifier': '99ddffff-2f73-46b0-92d1-614409d83001',
                'organization': {'name': 'Helsingin Yliopisto'}
            }
        }
    )
    # Create workspace with contents required by the tested task
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    create_test_data(workspace=workspace)

    # Init and run task
    task = CreateMets(workspace=workspace,
                      dataset_id='create_mets_dataset',
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created mets.xml
    tree = lxml.etree.parse(
        os.path.join(workspace, 'mets.xml')
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
    """Create data needed to run ``CreateMets`` task.

    :workspace: Workspace directory in which the data is created.
    """
    # Create directory structure
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Create dmdsec
    import_description(
        dmdsec_location='tests/data/datacite_sample.xml',
        workspace=sipdirectory
    )

    # Create provenance
    premis_event(
        event_type='creation', event_datetime='2016-10-13T12:30:55',
        event_detail='Poika, 2.985 kg', event_outcome='success',
        event_outcome_detail='Outcome detail',
        workspace=sipdirectory
    )

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object(
        workspace=sipdirectory,
        skip_wellformed_check=True,
        filepaths=[test_data_folder]
    )

    # Create structmap
    compile_structmap(workspace=sipdirectory)
