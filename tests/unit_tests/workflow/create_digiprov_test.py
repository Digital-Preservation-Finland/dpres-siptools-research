"""Test the :mod:`siptools_research.workflow.create_digiprov` module"""

import os

import pytest
import lxml

from siptools_research.workflow import create_digiprov
import tests.conftest


@pytest.mark.usefixtures("testmongoclient", "testmetax")
# pylint: disable=invalid-name
def test_createprovenanceinformation(testpath):
    """Tests for `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML file created
    - Log file is created

    :param testpath: Testpath fixture
    :returns: ``None``
    """

    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
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

    # Check that XML is created in workspace/sip-inprogrss/
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'creation-event-amd.xml'))

    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'metadata+modification-event-amd.xml'))

    # Check that target file is created
    with open(os.path.join(workspace, 'create-provenance-information.'
                           'finished')) as open_file:
        assert 'Dataset id=create_digiprov_test_dataset_file_and_logging' in\
            open_file.read()


@pytest.mark.usefixtures("testmongoclient", "testmetax")
# pylint: disable=invalid-name
def test_failed_createprovenanceinformation(testpath):
    """Test case where `CreateProvenanceInformation` task should fail.
    The dataset requested does not have all the required fields (date) in
    provenance information, which should cause exception.

    :param testpath: Testpath fixture
    :returns: ``None``
    """

    # Create empty workspace
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = create_digiprov.CreateProvenanceInformation(
        dataset_id="create_digiprov_test_dataset_date_data_missing",
        workspace=workspace,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run task.
    with pytest.raises(KeyError):
        task.run()
    assert not task.complete()

    # There should not be anything else in the workspace
    assert set(os.listdir(workspace)) == {'sip-in-progress', 'logs'}


@pytest.mark.usefixtures("testmetax")
def test_create_premis_events(testpath):
    """Test `create_premis_event` function. Output XML file should be produced
    and it should contain some specified elements.

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
    tree = lxml.etree.parse(os.path.join(testpath, 'creation-event-amd.xml'))

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
