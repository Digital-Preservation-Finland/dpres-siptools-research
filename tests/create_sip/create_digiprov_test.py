"""Test the `siptools_research.create_sip.create_digiprov` module"""

import os
import pymongo
import httpretty
import mongomock
from siptools_research.create_sip.create_digiprov \
    import CreateProvenanceInformation

DATASET_PATH = "tests/data/metax_datasets/"

def test_createprovenanceinformation(testpath, monkeypatch):
    """Test `CreateProvenanceInformation` task.

    :testpath: Testpath fixture
    :returns: None
    """

    mongoclient = mongomock.MongoClient()
    def mock_mongoclient(*args):
        """Returns already initialized mongomock.MongoClient"""
        return mongoclient

    monkeypatch.setattr(pymongo, 'MongoClient', mock_mongoclient)


    # Use fake http-server and local sample JSON-file instead real Metax-API.
    # @httpretty.activate decorator is not used because it does not work with
    # fixture
    httpretty.enable()
    data_file_name = "provenance_data.json"
    with open(os.path.join(DATASET_PATH, data_file_name)) as data_file:
        data = data_file.read()

    httpretty.register_uri(httpretty.GET,
                           "https://metax-test.csc.fi/rest/v1/datasets/1",
                           body=data,
                           status=200,
                           content_type='application/json'
                          )


    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'transfers'))

    # Create file in workspace directory
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, 'transfers', testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Init task
    task = CreateProvenanceInformation(home_path=workspace,
                                       workspace=workspace)
    assert not task.complete()

    # Run task. Task returns generator, so it must be iterated to really run
    # the code
    returned_tasks = task.run()
    for task in returned_tasks:
        pass
    assert task.complete()

    # Check that XML is created
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'creation-event.xml'))

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id':'workspace'}
    )
    print doc
    assert doc['_id'] == 'workspace'
    assert doc['wf_tasks']['create-provenance-information']['messages']\
        == 'Provenance metadata created.'
    assert doc['wf_tasks']['create-provenance-information']['result']\
        == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1


    # Disable fake http-server
    httpretty.disable()

    # TODO: Test for CreateProvenanceInformation.requires()

    # TODO: Test for DigiprovComplete.requires()

