"""Test the create_provenance_information method of CreateMets class."""

import copy
import json

import pytest
import lxml

from siptools_research.workflow.create_mets import CreateMets
from siptools_research.exceptions import InvalidDatasetMetadataError
import tests.metax_data
import tests.utils


@pytest.mark.usefixtures("testmongoclient")
@pytest.mark.parametrize(
    ["events", "expected_ids", "provenance_data"],
    [
        # 0 events
        (
            [], [], None
        ),
        # 1 event
        (
            ["creation"],
            ['6fc8a863bb6ed3cee2b1e853aa38d2db'],
            tests.metax_data.datasets.BASE_PROVENANCE
        ),
        # multiple events
        (
            ["creation", "metadata modification"],
            ['6fc8a863bb6ed3cee2b1e853aa38d2db',
             'f1ffc55803b971ab8dd013710766f47e'],
            tests.metax_data.datasets.BASE_PROVENANCE
        ),
        # provenance event made in Qvain
        (
            ['creation'],
            ['56e8efc54b66b68319a3db79e6038eb3'],
            tests.metax_data.datasets.QVAIN_PROVENANCE
        )
    ]
)
def test_createprovenanceinformation(workspace,
                                     requests_mock,
                                     events,
                                     expected_ids,
                                     provenance_data):
    """Test `create_provenance_information` method.

    - XML files are created
    - Metadata reference file is created

    :param workspace: Testpath fixture
    :param requests_mock: HTTP request mocker
    :param events: list of preservation events in dataset metadata
    :param expected_ids: expected identifiers of PREMIS events that are created
    :param provenance_data: The data used for creating provenance events.
    :returns: ``None``
    """
    # Mock metax. Create a dataset with provenance events.
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    dataset['research_dataset']['provenance'] = []
    for event in events:
        provenance = copy.deepcopy(provenance_data)
        if "preservation_event" in provenance:
            provenance["preservation_event"]["pref_label"]["en"] = event
        else:
            provenance["lifecycle_event"]["pref_label"]["en"] = event
        dataset['research_dataset']['provenance'].append(provenance)
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run the method.
    task.create_provenance_information()

    # PREMIS event XML should be created for each event
    sipdirectory = workspace / 'preservation' / 'sip-in-progress'
    created_files = {path.name for path in sipdirectory.iterdir()
                     if path.suffix == '.xml'}
    expected_files = {f'{id}-PREMIS%3AEVENT-amd.xml' for id in expected_ids}
    assert created_files == expected_files

    # Metadata reference file should have references to all created
    # premis events
    if events:
        references = json.loads(
            (workspace
             / "preservation" / "sip-in-progress"
             / "premis-event-md-references.jsonl").read_bytes()
        )
        assert set(references['.']['md_ids']) \
            == {f'_{id}' for id in expected_ids}


@pytest.mark.usefixtures("testmongoclient")
def test_failed_createprovenanceinformation(
        workspace, pkg_root, requests_mock):
    """Test `create_provenance_information` method failure.

    One of the provenance events of the dataset is invalid, which should
    cause exception.

    :param workspace: Test workspace directory fixture
    :param pkg_root: Test packaging root directory fixture
    :returns: ``None``
    """
    # Mock metax. Create a dataset with invalid provenance metadata.
    provenance = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
    del provenance["preservation_event"]
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['research_dataset']['provenance'] = [provenance]
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run method
    with pytest.raises(
        InvalidDatasetMetadataError,
        match="Provenance metadata does not have key 'preservation_event'"
    ):
        task.create_provenance_information()

    # No files should have been created in workspace directory and
    # temporary directories should cleaned
    assert {path.name for path in workspace.iterdir()} == {'preservation'}
    assert {path.name for path in (workspace / 'preservation').iterdir()} \
        == {'sip-in-progress'}
    assert not list((workspace / 'preservation' / 'sip-in-progress').iterdir())
    assert not list((pkg_root / 'tmp').iterdir())


@pytest.mark.parametrize(
    ['provenance_data', 'premis_id'],
    [
        [
            tests.metax_data.datasets.BASE_PROVENANCE,
            '6fc8a863bb6ed3cee2b1e853aa38d2db'
        ],
        [
            tests.metax_data.datasets.QVAIN_PROVENANCE,
            '56e8efc54b66b68319a3db79e6038eb3'
        ]
    ]
)
def test_create_premis_events(
    workspace, requests_mock, provenance_data, premis_id
):
    """Test XML files content.

    Output XML file should be produced and it should contain some
    specified elements.

    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param provenance_data: The data used for creating provenance events
    :param premis_id: The id created for the PREMIS event
    :returns: ``None``
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    dataset["research_dataset"]["provenance"] = [provenance_data]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Create provenance info xml-file to tempdir
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.create_provenance_information()

    # Check that the created xml-file contains correct elements.
    # pylint: disable=no-member
    tree = lxml.etree.parse(str(
        workspace / 'preservation' / 'sip-in-progress'
        / f'{premis_id}-PREMIS%3AEVENT-amd.xml'
    ))

    namespaces = {
        'mets': "http://www.loc.gov/METS/",
        'premis': "info:lc/xmlns/premis-v2"
    }
    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap',
                          namespaces=namespaces)
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventIdentifier'
                          '/premis:eventIdentifierType',
                          namespaces=namespaces)
    assert elements[0].text == "UUID"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventType',
                          namespaces=namespaces)
    assert elements[0].text == "creation"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventDateTime',
                          namespaces=namespaces)
    if "temporal" in provenance_data:
        assert elements[0].text == "2014-01-01T08:19:58Z"
    else:
        assert elements[0].text == "OPEN"

    # Title and description should be formatted together as "title:
    # description" or just as is if the other one does not exist
    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventDetail',
                          namespaces=namespaces)
    if "title" in provenance_data and "description" in provenance_data:
        assert elements[0].text == "Title: Description of provenance"
    elif "title" in provenance_data:
        assert elements[0].text == "Title"
    elif "description" in provenance_data:
        assert elements[0].text == "Description of provenance"
    else:
        # Invalid provenance, there is no title or description
        assert False

    # Outcome should be "unknown" if missing
    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event'
                          '/premis:eventOutcomeInformation'
                          '/premis:eventOutcome',
                          namespaces=namespaces)
    if "event_outcome" in provenance_data:
        assert elements[0].text == "outcome"
    else:
        assert elements[0].text == "unknown"

    # Outcome description is optional
    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event'
                          '/premis:eventOutcomeInformation'
                          '/premis:eventOutcomeDetail'
                          '/premis:eventOutcomeDetailNote',
                          namespaces=namespaces)
    if "outcome_description" in provenance_data:
        assert elements[0].text == "outcome_description"
    else:
        assert elements == []
