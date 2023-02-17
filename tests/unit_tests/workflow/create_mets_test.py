"""Tests for module :mod:`siptools_research.workflow.create_mets`."""
from pathlib import Path

import lxml
import pytest
from siptools.scripts.compile_structmap import compile_structmap
from siptools.scripts.import_description import import_description
from siptools.scripts.import_object import import_object
from siptools.scripts.premis_event import premis_event

import tests.utils
from siptools_research.workflow.create_mets import CreateMets

NAMESPACES = {
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
    'mets': "http://www.loc.gov/METS/",
    'fi': "http://digitalpreservation.fi/schemas/mets/fi-extensions",
    'premis': "info:lc/xmlns/premis-v2",
    'xlink': "http://www.w3.org/1999/xlink"
}


METS_ATTRIBUTES = {
    'PROFILE': 'http://digitalpreservation.fi/mets-profiles/research-data',
    f"{{{NAMESPACES['fi']}}}CONTRACTID": "urn:uuid:abcd1234-abcd-1234-5678"
                                         "-abcd1234abcd",
    f"{{{NAMESPACES['xsi']}}}schemaLocation": 'http://www.loc.gov/METS/ '
                                              'http://digitalpreservation.fi/'
                                              'schemas/mets/mets.xsd',
    f"{{{NAMESPACES['fi']}}}SPECIFICATION": '1.7.5',
    'OBJID': 'doi:test',
    f"{{{NAMESPACES['fi']}}}CATALOG": '1.7.5',
}


@pytest.mark.usefixtures('testmongoclient')
def test_create_mets_ok(workspace, requests_mock):
    """Test the workflow task CreateMets.

    :param pkg_root: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock)

    # Create workspace with contents required by the tested task
    create_test_data(workspace=workspace)

    # Init and run task
    task = CreateMets(workspace=str(workspace),
                      dataset_id='dataset_identifier',
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created mets.xml
    tree = lxml.etree.parse(str(workspace / 'mets.xml'))

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
        == "Testiorganisaatio"
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
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir(parents=True)

    # Create dmdsec
    import_description(
        dmdsec_location='tests/data/datacite_sample.xml',
        workspace=str(sipdirectory)
    )

    # Create provenance
    premis_event(
        event_type='creation', event_datetime='2016-10-13T12:30:55',
        event_detail='Poika, 2.985 kg', event_outcome='success',
        event_outcome_detail='Outcome detail',
        workspace=str(sipdirectory)
    )

    # Create tech metadata
    test_data_folder = Path('./tests/data/structured').resolve()
    import_object(
        workspace=str(sipdirectory),
        skip_wellformed_check=True,
        filepaths=[str(test_data_folder)]
    )

    # Create structmap
    compile_structmap(workspace=str(sipdirectory))
