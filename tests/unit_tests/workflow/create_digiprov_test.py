"""Test the create_provenance_information method of CreateMets class."""

import copy

import pytest
import lxml

from siptools_research.workflow.create_mets import CreateMets
from siptools_research.exceptions import InvalidDatasetMetadataError
import tests.metax_data
import tests.utils


NAMESPACES = {
    'mets': "http://www.loc.gov/METS/",
    'premis': "info:lc/xmlns/premis-v2"
}


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
    """Test creating METS document with multiple provenance events.

    Premis event should be created for each event.

    :param workspace: Testpath fixture
    :param requests_mock: HTTP request mocker
    :param events: list of preservation events in dataset metadata
    :param expected_ids: expected identifiers of PREMIS events that are
                         created
    :param provenance_data: The data used for creating provenance
                            events.
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

    # Init and run task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()
    assert task.complete()

    # PREMIS event should be created for each event
    mets = lxml.etree.parse(str(workspace / "preservation" / "mets.xml"))
    digiprovmd_elements = mets.xpath('/mets:mets/mets:amdSec/mets:digiprovMD',
                                     namespaces=NAMESPACES)
    digiprovmd_identifiers \
        = [elem.attrib['ID'].strip('_') for elem in digiprovmd_elements]
    for expected_id in expected_ids:
        assert expected_id in digiprovmd_identifiers

    # PREMIS events should be refenced in structure map
    structmap_root_div_elements \
        = mets.xpath('/mets:mets/mets:structMap/mets:div',
                     namespaces=NAMESPACES)
    structmap_references = [
        identifier.strip("_")
        for identifier
        in structmap_root_div_elements[0].attrib.get('ADMID', "").split()
    ]
    for expected_id in expected_ids:
        assert expected_id in structmap_references


@pytest.mark.usefixtures("testmongoclient")
def test_failed_createprovenanceinformation(workspace, requests_mock):
    """Test provenance event creation failure.

    One of the provenance events of the dataset is invalid, which should
    cause exception.

    :param workspace: Test workspace directory fixture
    :param requests_mock: HTTP request mocker
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

    # Run task
    with pytest.raises(
        InvalidDatasetMetadataError,
        match="Provenance metadata does not have key 'preservation_event'"
    ):
        task.run()

    # Task should not be complete
    assert not task.complete()
    assert not (workspace / 'preservation/mets.xml').exists()


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
    """Test creating METS with provenance event.

    Output XML file should be produced and it should contain some
    specified elements.

    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param provenance_data: The data used for creating provenance events
    :param premis_id: The id created for the PREMIS event
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    dataset["research_dataset"]["provenance"] = [provenance_data]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Run task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()

    # Find the digiprovMD element of provenance event from METS document
    mets = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))
    digiprovmd = mets.xpath(
        f"/mets:mets/mets:amdSec/mets:digiprovMD[@ID='_{premis_id}']",
        namespaces=NAMESPACES
    )[0]

    # Check that created  digiprovMD element contains correct elements.
    elements = digiprovmd.xpath('mets:mdWrap', namespaces=NAMESPACES)
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventIdentifier'
        '/premis:eventIdentifierType',
        namespaces=NAMESPACES
    )
    assert elements[0].text == "UUID"

    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventType',
        namespaces=NAMESPACES
    )
    assert elements[0].text == "creation"

    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventDateTime',
        namespaces=NAMESPACES
    )
    if "temporal" in provenance_data:
        assert elements[0].text == "2014-01-01T08:19:58Z"
    else:
        assert elements[0].text == "OPEN"

    # Title and description should be formatted together as "title:
    # description" or just as is if the other one does not exist
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventDetail',
        namespaces=NAMESPACES
    )
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
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcome',
        namespaces=NAMESPACES)
    if "event_outcome" in provenance_data:
        assert elements[0].text == "outcome"
    else:
        assert elements[0].text == "unknown"

    # Outcome description is optional
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcomeDetail'
        '/premis:eventOutcomeDetailNote',
        namespaces=NAMESPACES
    )
    if "outcome_description" in provenance_data:
        assert elements[0].text == "outcome_description"
    else:
        assert elements == []
