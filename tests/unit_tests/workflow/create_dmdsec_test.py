"""Tests for create_descriptive_metadata method of CreateMets task."""
import json
import copy

import pytest
from lxml import etree

import tests.utils
from tests.metax_data.datasets import BASE_DATASET
from siptools_research.workflow.create_mets import CreateMets


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata(workspace, requests_mock):
    """Test the `create_descriptive_metadata` method.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run method
    task.create_descriptive_metadata()

    # Check that XML is created in sip creation directory and it
    # contains correct elements.
    dmdsecfile = workspace / 'preservation' / 'sip-in-progress' / 'dmdsec.xml'
    dmdsec = etree.parse(str(dmdsecfile))
    namespaces = {'mets': "http://www.loc.gov/METS/",
                  'datacite': "http://datacite.org/schema/kernel-4"}

    # The mdWrap element should contain the datacite metadata
    mdwrap = dmdsec.xpath('/mets:mets//mets:dmdSec/mets:mdWrap',
                          namespaces=namespaces)
    assert mdwrap[0].attrib["OTHERMDTYPE"] == "DATACITE"
    assert mdwrap[0].attrib["MDTYPEVERSION"] == "4.1"
    mets_datacite = mdwrap[0].xpath('mets:xmlData/datacite:resource',
                                    namespaces=namespaces)[0]

    # Compare datacite metadata in METS file to the original datacite
    # metadata retrieved from metax. First rip the datacite from METS
    # and lean up extra namespaces.
    mets_datacite = etree.fromstring(etree.tostring(mets_datacite))
    etree.cleanup_namespaces(mets_datacite)
    # Compare XMLs. The string presertations should be indentical
    metax_datacite = tests.metax_data.datasets.BASE_DATACITE.getroot()
    assert etree.tostring(mets_datacite) == etree.tostring(metax_datacite)

    # Check that descriptive metadata reference file is created in sip
    # creation directory and it contains correct elements
    import_description_path = (
        workspace / 'preservation' / 'sip-in-progress'
        / 'import-description-md-references.jsonl'
    )
    references = json.loads(import_description_path.read_bytes())
    assert references['.']["path_type"] == "directory"
    assert references['.']["streams"] == {}
    assert len(references['.']["md_ids"]) == 1

    # Premis event reference file should be created in sip-in-progress
    # directory
    premis_event_reference_file = workspace / "preservation" / \
        "sip-in-progress" / "premis-event-md-references.jsonl"
    references = json.loads(premis_event_reference_file.read_bytes())
    assert len(references['.']["md_ids"]) == 1
    premis_event_identifier = references['.']["md_ids"][0][1:]

    # SIP creation directory should contain only descriptive metadata
    # XML, premis event reference file, descriptive metadata reference
    # file and premis event XML.
    files = {path.name for path
             in (workspace / "preservation" / "sip-in-progress").iterdir()}
    assert files == {
        'dmdsec.xml',
        "premis-event-md-references.jsonl",
        'import-description-md-references.jsonl',
        f'{premis_event_identifier}-PREMIS%3AEVENT-amd.xml'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata_invalid_datacite(workspace, requests_mock):
    """Test `create_descriptive_metadata` method failure.

    The method fails when datacite metadata is not valid. Nothing should
    be written in `sip-in-progress` directory.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
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
        task.create_descriptive_metadata()

    # Nothing should be written in `sip-in-progress` directory
    assert not list((workspace / 'preservation' / 'sip-in-progress').iterdir())
