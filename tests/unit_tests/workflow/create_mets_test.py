"""Tests for module :mod:`siptools_research.workflow.create_mets`."""
import copy

import lxml
import pytest

import tests.utils
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TXT_FILE
from siptools_research.workflow.create_mets import CreateMets

NAMESPACES = {
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
    'mets': "http://www.loc.gov/METS/",
    'fi': "http://digitalpreservation.fi/schemas/mets/fi-extensions",
    'premis': "info:lc/xmlns/premis-v2",
    'xlink': "http://www.w3.org/1999/xlink"
}


@pytest.mark.parametrize(
    'data_catalog,objid',
    [
        ('urn:nbn:fi:att:data-catalog-ida', 'doi:pas-version-id'),
        ('urn:nbn:fi:att:data-catalog-pas', 'doi:test')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_mets_ok(workspace, requests_mock, data_catalog, objid):
    """Test the workflow task CreateMets.

    Test creating METS for a simple dataset that contains one text file.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :param data_catalog: Data catalog identifier of dataset
    :param objid: Identifier expected to be used as OBJID
    :returns: ``None``
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    files = [copy.deepcopy(TXT_FILE)]
    dataset['identifier'] = workspace.name
    dataset['data_catalog']['identifier'] = data_catalog
    dataset['preservation_dataset_version'] \
        = {'preferred_identifier': 'doi:pas-version-id'}
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created mets.xml
    tree = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))

    # Check that the root element contains expected attributes.
    mets_attributes = {
        'PROFILE': 'http://digitalpreservation.fi/mets-profiles/research-data',
        f"{{{NAMESPACES['fi']}}}CONTRACTID":
        "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
        f"{{{NAMESPACES['xsi']}}}schemaLocation":
        'http://www.loc.gov/METS/ http://digitalpreservation.fi/'
        'schemas/mets/mets.xsd',
        f"{{{NAMESPACES['fi']}}}SPECIFICATION": '1.7.6',
        'OBJID': objid,
        f"{{{NAMESPACES['fi']}}}CONTENTID": objid,
        f"{{{NAMESPACES['fi']}}}CATALOG": '1.7.6',
    }
    assert tree.getroot().attrib == mets_attributes

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
