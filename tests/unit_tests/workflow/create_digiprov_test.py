"""Test the :mod:`siptools_research.workflow.create_digiprov` module."""

import copy
import json

import pytest
import lxml

from siptools_research.workflow import create_digiprov
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
            ['7c727916e1f530bba180d3040b40e5c7'],
            tests.metax_data.datasets.QVAIN_PROVENANCE
        )
    ]
)
def test_createprovenanceinformation(pkg_root,
                                     workspace,
                                     requests_mock,
                                     events,
                                     expected_ids,
                                     provenance_data):
    """Test `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML files are created
    - Metadata reference file is created

    :param pkg_root: Testpath fixture
    :param workspace: Testpath fixture
    :param requests_mock: HTTP request mocker
    :param events: list of preservation events in dataset metadata
    :param expected_ids: expected identifiers of PREMIS events that are created
    :param provenance_data: The data used for creating provenance events.
    :returns: ``None``
    """
    # Mock metax. Create a dataset with provenance events.
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['research_dataset']['provenance'] = []
    for event in events:
        provenance = copy.deepcopy(provenance_data)
        if "preservation_event" in provenance:
            provenance["preservation_event"]["pref_label"]["en"] = event
        else:
            provenance["lifecycle_event"]["pref_label"]["en"] = event
        dataset['research_dataset']['provenance'].append(provenance)
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Create workspace with required directories
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir()

    # Init task
    task = create_digiprov.CreateProvenanceInformation(
        workspace=str(workspace),
        dataset_id="dataset_identifier",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # PREMIS event XML should be created for each event
    created_files = {path.name for path in sipdirectory.iterdir()}
    expected_files = {f'{id}-PREMIS%3AEVENT-amd.xml' for id in expected_ids}
    assert created_files == expected_files

    # Metadata reference file should have references to all created
    # premis events
    references = json.loads(
        (workspace / "create-provenance-information.jsonl").read_bytes()
    )
    assert set(references['.']['md_ids']) == {f'_{id}' for id in expected_ids}

    # Temporary directories should be removed
    assert not list((pkg_root / "tmp").iterdir())


@pytest.mark.usefixtures("testmongoclient")
def test_failed_createprovenanceinformation(
        workspace, pkg_root, requests_mock):
    """Test `CreateProvenanceInformation` task failure.

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
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Create empty workspace
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir()

    # Init task
    task = create_digiprov.CreateProvenanceInformation(
        dataset_id="dataset_identifier",
        workspace=str(workspace),
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run task.
    with pytest.raises(
        InvalidDatasetMetadataError,
        match="Provenance metadata does not have key 'preservation_event'"
    ):
        task.run()
    assert not task.complete()

    # No files should have been created in workspace directory and
    # temporary directories should cleaned
    files = {path.name for path in workspace.iterdir()}
    assert files == {'sip-in-progress'}
    assert not list(sipdirectory.iterdir())
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
            '7c727916e1f530bba180d3040b40e5c7'
        ]
    ]
)
def test_create_premis_events(
    pkg_root, requests_mock, provenance_data, premis_id
):
    """Test `create_premis_event` function.

    Output XML file should be produced and it should contain some
    specified elements.

    :param pkg_root: Test packaging directory fixture
    :param requests_mock: HTTP request mocker
    :param provenance_data: The data used for creating provenance events
    :param premis_id: The id created for the PREMIS event
    :returns: ``None``
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["research_dataset"]["provenance"] = [provenance_data]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Create provenance info xml-file to tempdir
    # pylint: disable=protected-access
    create_digiprov._create_premis_events(
        'dataset_identifier',
        str(pkg_root),
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Check that the created xml-file contains correct elements.
    # pylint: disable=no-member
    tree = lxml.etree.parse(str(
        pkg_root / f'{premis_id}-PREMIS%3AEVENT-amd.xml'
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
    assert elements[0].text == "2014-01-01T08:19:58Z"

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
        assert elements[0].text == "UNKNOWN"

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
