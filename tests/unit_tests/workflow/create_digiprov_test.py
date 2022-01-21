"""Test the :mod:`siptools_research.workflow.create_digiprov` module."""

import copy
import json

import pytest
import lxml

from siptools_research.workflow import create_digiprov
import tests.metax_data
import tests.utils


@pytest.mark.usefixtures("testmongoclient")
def test_createprovenanceinformation(pkg_root, workspace, requests_mock):
    """Test `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML files are created
    - Metadata reference file is created

    :param pkg_root: Testpath fixture
    :param workspace: Testpath fixture
    :returns: ``None``
    """
    # Mock metax. Create a dataset with two provenance events
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    provenance = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
    provenance["preservation_event"]["pref_label"]["en"] \
        = "metadata modification"
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

    # Check that XMLs are created in workspace/sip-inprogress/
    files = set(path.name for path in sipdirectory.iterdir())
    assert (
        files == set(
            ['6fc8a863bb6ed3cee2b1e853aa38d2db-PREMIS%3AEVENT-amd.xml',
             'f1ffc55803b971ab8dd013710766f47e-PREMIS%3AEVENT-amd.xml']
        )
    )

    # Metadata reference file should have references to both premis
    # events
    references = json.loads(
        (workspace / "create-provenance-information.jsonl").read_bytes()
    )
    assert set(references['.']['md_ids']) \
        == set(['_6fc8a863bb6ed3cee2b1e853aa38d2db',
                '_f1ffc55803b971ab8dd013710766f47e'])

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
    dataset['research_dataset']['provenance'].append(provenance)
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
    with pytest.raises(KeyError, match='preservation_event'):
        task.run()
    assert not task.complete()

    # No files should have been created in workspace directory and
    # temporary directories should cleaned
    files = set(path.name for path in workspace.iterdir())
    assert files == {'sip-in-progress'}
    assert not list(sipdirectory.iterdir())
    assert not list((pkg_root / 'tmp').iterdir())


@pytest.mark.usefixtures('mock_metax_access')
def test_create_premis_events(pkg_root):
    """Test `create_premis_event` function.

    Output XML file should be produced and it should contain some
    specified elements.

    :param pkg_root: Test packaging directory fixture
    :returns: ``None``
    """
    # Create provenance info xml-file to tempdir
    # pylint: disable=protected-access
    create_digiprov._create_premis_events(
        'create_digiprov_test_dataset_detailed_check',
        str(pkg_root),
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Check that the created xml-file contains correct elements.
    # pylint: disable=no-member
    tree = lxml.etree.parse(str(
        pkg_root / '24d4d306da97c4fd31c5ff1cc8c28316-PREMIS%3AEVENT-amd.xml'
    ))

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventIdentifier'
                          '/premis:eventIdentifierType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "UUID"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "creation"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventDateTime',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "2014-01-01T08:19:58Z"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event/premis:eventDetail',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "Description of provenance"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event'
                          '/premis:eventOutcomeInformation'
                          '/premis:eventOutcome',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "success"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'
                          '/mets:xmlData/premis:event'
                          '/premis:eventOutcomeInformation'
                          '/premis:eventOutcomeDetail'
                          '/premis:eventOutcomeDetailNote',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"})
    assert elements[0].text == "This is a detail of an successful event"
