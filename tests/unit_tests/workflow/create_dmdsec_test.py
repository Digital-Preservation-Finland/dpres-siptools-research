"""Tests for :mod:`siptools_research.workflow.create_dmdsec` module."""
import json

import pytest
from lxml import etree

import tests.utils
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata(workspace, requests_mock):
    """Test `CreateDescriptiveMetadata` task.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    # Init task
    task = CreateDescriptiveMetadata(
        dataset_id="dataset_identifier",
        workspace=str(workspace),
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

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

    # Premis event reference file should be created in workspace
    # directory
    premis_event_reference_file = \
        workspace / "preservation" / 'create-descriptive-metadata.jsonl'
    references = json.loads(premis_event_reference_file.read_bytes())
    assert len(references['.']["md_ids"]) == 1
    premis_event_identifier = references['.']["md_ids"][0][1:]

    # SIP creation directory should contain only descriptive metadata
    # XML, descriptive metadata reference file and premis event XML.
    files = {path.name for path
             in (workspace / "preservation" / "sip-in-progress").iterdir()}
    assert files == {
        'dmdsec.xml',
        'import-description-md-references.jsonl',
        f'{premis_event_identifier}-PREMIS%3AEVENT-amd.xml'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata_invalid_datacite(workspace, requests_mock):
    """Test `CreateDescriptiveMetadata` task failure.

    The task fails when datacite metadata is not valid. Nothing should
    be written in `sip-in-progress` directory.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Create dataset that contains invalid datacite metadata
    datacite = etree.Element("{foo}bar")
    tests.utils.add_metax_dataset(requests_mock, datacite=datacite)

    # Init task
    task = CreateDescriptiveMetadata(
        dataset_id="dataset_identifier",
        workspace=str(workspace),
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Task should fail
    with pytest.raises(TypeError, match='Invalid namespace: foo'):
        task.run()
    assert not task.complete()

    # Nothing should be written in `sip-in-progress` directory
    assert not list((workspace / 'preservation' / 'sip-in-progress').iterdir())
