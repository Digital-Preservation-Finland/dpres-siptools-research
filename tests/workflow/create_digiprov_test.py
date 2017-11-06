"""Test the `siptools_research.workflow.create_digiprov` module"""

import os
import pymongo
import lxml
from siptools_research.workflow import create_digiprov

# pylint: disable=unused-argument,invalid-name,fixme
def test_createprovenanceinformation(testpath, testmongoclient, testmetax):
    """Tests for `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML file created
    - Log file is created
    - Log entry is created to mongodb


    :testpath: Testpath fixture
    :testmongoclient: Pymongo mock fixture
    :testmetax: Fake metax fixture
    :returns: None
    """

    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = testpath
    # TODO: The task should be able to create 'logs' directory if it does not
    # exist. Therefore this line should be unnecessary.
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'transfers'))

    # Create file in workspace directory
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, 'transfers', testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Init task
    task = create_digiprov.CreateProvenanceInformation(workspace=workspace,
                                                       dataset_id="1")
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML is created in workspace/sip-inprogrss/
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'creation-event.xml'))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-provenance-information.log'))\
            as open_file:
        # Check last line of log file
        assert open_file.readlines()[-1].startswith("premis_event created")

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id': os.path.basename(workspace)}
    )
    assert doc['_id'] == os.path.basename(workspace)
    assert doc['workflow_tasks']['CreateProvenanceInformation']['messages']\
        == 'Provenance metadata created.'
    assert doc['workflow_tasks']['CreateProvenanceInformation']['result']\
        == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1


def test_failed_createprovenanceinformation(testpath, testmongoclient):
    """Test case where `CreateProvenanceInformation` task should fail.
    The workspace is empty, which should cause exception. However, the task
    should write new log entry to mongodb.

    :testpath: Testpath fixture
    :testmongoclient: Pymongo mock fixture
    :returns: None
    """

    # Create empty workspace
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(os.path.join(workspace, 'transfers'))
    os.makedirs(os.path.join(workspace, 'logs'))

    # Create file in workspace directory
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, 'transfers', testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Init task
    task = create_digiprov.CreateProvenanceInformation(dataset_id="1",
                                                       workspace=workspace)

    # Run task.
    task.run()
    assert not task.complete()

    # There should not be anything else in the workspace
    assert set(os.listdir(workspace)) == {'transfers', 'logs'}
    assert set(os.listdir(os.path.join(workspace, 'logs'))) == {
        'task-create-provenance-information.log'
    }

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id': os.path.basename(workspace)}
    )
    assert doc['_id'] == os.path.basename(workspace)
    assert 'Could not create procenance metada, element "provenance" not '\
            'found from metadata.' in\
        doc['workflow_tasks']['CreateProvenanceInformation']['messages']
    assert doc['workflow_tasks']['CreateProvenanceInformation']['result']\
        == 'failure'
    assert mongoclient['siptools-research'].workflow.count() == 1


def test_create_premis_event(testpath, testmetax):
    """Test `create_premis_event` function. Output XML file should be produced
    and it should contain some specified elements.

    :testpath: Testpath fixture
    :testmetax: Fake metax fixture
    :returns: None
    """

    # Create provenance info xml-file to tempdir
    workspace = testpath
    create_digiprov.create_premis_event('1', workspace)

    # Check that the created xml-file contains correct elements.
    tree = lxml.etree.parse(os.path.join(testpath, 'creation-event.xml'))

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventIdentifier'\
                          '/premis:eventIdentifierType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "UUID"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventType',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "creation"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventDateTime',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "2014-01-01T08:19:58Z"

    elements = tree.xpath('/mets:mets/mets:amdSec/mets:digiprovMD/mets:mdWrap'\
                          '/mets:xmlData/premis:event/premis:eventDetail',
                          namespaces={'mets': "http://www.loc.gov/METS/",
                                      'premis': "info:lc/xmlns/premis-v2"}
                         )
    assert elements[0].text == "Description of provenance"


    # TODO: Test for CreateProvenanceInformation.requires()

    # TODO: Test for CreateProvenanceInformation.output()

    # TODO: Test for DigiprovComplete.output()
