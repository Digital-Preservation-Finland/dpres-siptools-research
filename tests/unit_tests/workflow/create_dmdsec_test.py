"""Tests for create_descriptive_metadata method of CreateMets task."""
import copy

import pytest
from lxml import etree

import tests.utils
from tests.metax_data.datasets import BASE_DATASET
from siptools_research.workflow.create_mets import CreateMets

NAMESPACES = {"mets": "http://www.loc.gov/METS/",
              "datacite": "http://datacite.org/schema/kernel-4",
              "premis": "info:lc/xmlns/premis-v2"}


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata(workspace, requests_mock):
    """Test descriptive metadata creation.

    Datacite XML should be imported to dmdSec of METS. DmdSec should be
    referenced in structure maps. Premis event should be created for
    datacite import.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init and runtask
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()
    assert task.complete()

    # Check that METS document contains correct elements.
    mets = etree.parse(str(workspace / 'preservation/mets.xml'))

    # The mdWrap element should contain the datacite metadata
    dmdsec = mets.xpath('/mets:mets//mets:dmdSec', namespaces=NAMESPACES)[0]
    mdwrap = dmdsec.xpath('mets:mdWrap', namespaces=NAMESPACES)
    assert mdwrap[0].attrib["OTHERMDTYPE"] == "DATACITE"
    assert mdwrap[0].attrib["MDTYPEVERSION"] == "4.1"
    mets_datacite = mdwrap[0].xpath('mets:xmlData/datacite:resource',
                                    namespaces=NAMESPACES)[0]

    # Compare datacite metadata in METS file to the original datacite
    # metadata retrieved from metax. First rip the datacite from METS
    # and lean up extra namespaces.
    mets_datacite = etree.fromstring(etree.tostring(mets_datacite))
    etree.cleanup_namespaces(mets_datacite)
    # Compare XMLs. The string presertations should be indentical
    metax_datacite = tests.metax_data.datasets.BASE_DATACITE.getroot()
    assert etree.tostring(mets_datacite) == etree.tostring(metax_datacite)

    # Check that descriptive metadata is referenced in both structMaps
    # (Fairdata-physical and Fairdata-logical)
    dmdsec_id = dmdsec.attrib["ID"]
    structmaps = mets.xpath("/mets:mets/mets:structMap", namespaces=NAMESPACES)
    assert len(structmaps) == 2
    for structmap in structmaps:
        structmap_div = structmap.xpath("mets:div", namespaces=NAMESPACES)[0]
        assert structmap_div.attrib["DMDID"] == dmdsec_id

    # Check that premis event is created for descriptive metadata import
    extraction_events \
        = mets.xpath('//premis:event[premis:eventType="metadata extraction"]',
                     namespaces=NAMESPACES)
    assert len(extraction_events) == 1
    event_detail = extraction_events[0].xpath('premis:eventDetail',
                                              namespaces=NAMESPACES)[0]
    assert event_detail.text \
        == 'Descriptive metadata import from external source'


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata_invalid_datacite(workspace, requests_mock):
    """Test importing invalid Datacite XML.

    The METS creation should fail when datacite metadata is not valid.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Create dataset that contains invalid datacite metadata
    datacite = etree.Element("{foo}bar")
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  datacite=datacite)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # The method should fail
    with pytest.raises(TypeError, match='Invalid namespace: foo'):
        task.run()

    assert not task.complete()
