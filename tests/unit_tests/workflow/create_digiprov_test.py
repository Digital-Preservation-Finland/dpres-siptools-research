"""Test the :mod:`siptools_research.workflow.create_digiprov` module."""

import copy
import json
import os

import pytest
import lxml

from siptools_research.workflow import create_digiprov
import tests.conftest
import tests.metax_data


@pytest.mark.usefixtures("testmongoclient", 'mock_metax_access')
# pylint: disable=invalid-name
def test_createprovenanceinformation(testpath):
    """Test `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML files are created
    - Metadata reference file is created

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    # Create workspace with required directories
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = create_digiprov.CreateProvenanceInformation(
        workspace=workspace,
        dataset_id="create_digiprov_test_dataset_file_and_logging",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML is created in workspace/sip-inprogress/
    assert os.path.isfile(os.path.join(
        workspace, 'sip-in-progress',
        '6fc8a863bb6ed3cee2b1e853aa38d2db-PREMIS%3AEVENT-amd.xml'))

    assert os.path.isfile(os.path.join(
        workspace, 'sip-in-progress',
        'f1ffc55803b971ab8dd013710766f47e-PREMIS%3AEVENT-amd.xml'))

    # Check that Metadata references file is created
    with open(os.path.join(workspace, 'sip-in-progress',
                           'premis-event-md-references.jsonl')) as file_:
        references = json.load(file_)
        assert set(references['.']['md_ids']) \
            == set(['_6fc8a863bb6ed3cee2b1e853aa38d2db',
                    '_f1ffc55803b971ab8dd013710766f47e'])


@pytest.mark.usefixtures("testmongoclient")
# pylint: disable=invalid-name
def test_failed_createprovenanceinformation(testpath, requests_mock):
    """Test `CreateProvenanceInformation` task failure.

    One of the provenance events of the dataset is invalid, which should
    cause exception.

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    # Mock metax. Create a dataset with invalid provenance metadata.
    provenance = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
    del provenance["preservation_event"]
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['research_dataset']['provenance'].append(provenance)
    tests.conftest.mock_metax_dataset(requests_mock, dataset=dataset)

    # Create empty workspace
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = create_digiprov.CreateProvenanceInformation(
        dataset_id="dataset_identifier",
        workspace=workspace,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run task.
    with pytest.raises(KeyError, match='preservation_event'):
        task.run()
    assert not task.complete()

    # There should not be anything else in the workspace, and the SIP
    # creation directory should be empty.
    assert set(os.listdir(workspace)) == {'sip-in-progress'}
    assert not os.listdir(os.path.join(workspace, 'sip-in-progress'))


@pytest.mark.usefixtures('mock_metax_access')
def test_create_premis_events(testpath):
    """Test `create_premis_event` function.

    Output XML file should be produced and it should contain some
    specified elements.

    :param testpath: Testpath fixture
    :returns: ``None``
    """
    # Create provenance info xml-file to tempdir
    workspace = testpath
    # pylint: disable=protected-access
    create_digiprov._create_premis_events(
        'create_digiprov_test_dataset_detailed_check',
        workspace,
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Check that the created xml-file contains correct elements.
    # pylint: disable=no-member
    tree = lxml.etree.parse(os.path.join(
        testpath, '24d4d306da97c4fd31c5ff1cc8c28316-PREMIS%3AEVENT-amd.xml'))

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
