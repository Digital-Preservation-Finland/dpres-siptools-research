"""Test the `siptools_research.workflow.create_digiprov` module"""

import os
import pymongo
import lxml
from siptools_research.workflow import create_digiprov

DATASET_PATH = "tests/data/metax_datasets/"
SAMPLE_CREATION_EVENT_PATH = "tests/data/sample_creation_event.xml"

# pylint: disable=unused-argument,invalid-name,fixme
def test_createprovenanceinformation(testpath, testmongoclient, testmetax):
    """Tests for `CreateProvenanceInformation` task.

    - `Task.complete()` is true after `Task.run()`
    - XML file created
    - Task output file is created
    - Log file is created
    - Log entry is created to mongodb


    :testpath: Testpath fixture
    :testmongoclient: Pymongo mock fixture
    :testmetax: Fake metax fixture
    :returns: None
    """

    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
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
    task = create_digiprov.CreateProvenanceInformation(home_path=testpath,
                                                       workspace=workspace)
    assert not task.complete()

    # Run task. Task returns generator, so it must be iterated to really run
    # the code
    returned_tasks = task.run()
    for task in returned_tasks:
        pass
    assert task.complete()

    # Check that XML is created in workspace/sip-inprogrss/
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'creation-event.xml'))

    # Check that task output file is created in workspace/task-output-files/
    assert os.path.isfile(os.path.join(workspace,
                                       'task-output-files',
                                       'create-provenance-information'))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-provenance-information.log'))\
            as open_file:
        assert open_file.read().startswith("premis_event created")

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id':'workspace'}
    )
    assert doc['_id'] == 'workspace'
    assert doc['wf_tasks']['create-provenance-information']['messages']\
        == 'Provenance metadata created.'
    assert doc['wf_tasks']['create-provenance-information']['result']\
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
    task = create_digiprov.CreateProvenanceInformation(home_path=testpath,
                                                       workspace=workspace)

    # Run task. Task returns generator, so it must be iterated to really run
    # the code
    returned_tasks = task.run()
    for task in returned_tasks:
        pass
    assert not task.complete()

    # Check that log is created
    with open(os.path.join(workspace,
                           'logs',
                           'task-failure.log')) as open_file:
        assert open_file.read() == "Task create-digiprov failed."

    # There should not be anything else in the workspace
    assert set(os.listdir(workspace)) == {'transfers', 'logs'}
    assert set(os.listdir(os.path.join(workspace, 'logs'))) == {
        'task-create-provenance-information.log',
        'task-failure.log'
    }

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id':'workspace'}
    )
    assert doc['_id'] == 'workspace'
    assert 'Could not create procenance metada, element "provenance" not '\
            'found from metadata.' in\
        doc['wf_tasks']['create-provenance-information']['messages']
    assert doc['wf_tasks']['create-provenance-information']['result']\
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