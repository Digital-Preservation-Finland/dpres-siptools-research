"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
import getpass
import pytest
import pymongo

#from tests.assertions import task_ok

#from siptools_research.target import mongo_settings

from siptools_research.workflow.create_techmd import CreateTechnicalMetadata

DATASET_PATH = "tests/data/metax_datasets/"
SAMPLE_CREATION_EVENT_PATH = "tests/data/sample_techmd.xml"


#def test_create_techmd_ok(testpath, testmongoclient, testmetax):
def test_create_techmd_ok(testpath, testmongoclient):
    """Test the workflow task CreateTechnicalMetadata module.
    """

    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = os.path.join(testpath, 'workspace')
    sip_creation_path = os.path.join(workspace, 'sip-in-progress')

    os.makedirs(workspace)
    # TODO: The task should be able to create 'logs' directory if it does not
    # exist. Therefore this line should be unnecessary.
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'transfers'))

    # Copy sample files to workspace directory
    shutil.copytree('tests/data/files/',
             os.path.join(workspace, 'sip-in-progress'))

    # Create file in workspace directory
    #testfilename = "aineisto"
    #testfilepath = os.path.join(workspace, 'transfers', testfilename)
    #with open(testfilepath, 'w') as testfile:
    #    testfile.write('11')
    #assert os.path.isfile(testfilepath)


    #workspace = os.path.join(workspace_root, os.listdir(workspace_root)[0])

    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="3",
                                   sip_creation_path=sip_creation_path,
                                   home_path=testpath)

    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()



    # Run task. Task returns generator, so it must be iterated to really run
    # the code
    #returned_tasks = task.run()
    #print returned_tasks
    #for task in returned_tasks:
    #    pass
    #assert task.complete()

    # Check that XML is created in workspace/sip-inprogrss/
    assert os.path.isfile(os.path.join(workspace,
        'project_x_FROZEN%2Fsome%2Fpath%2Ffile_name_5-techmd.xml'))

    # Check that task output file is created in workspace/task-output-files/
    assert os.path.isfile(os.path.join(workspace,
        'task-output-files',
        'create-technical-metadata'))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
            'logs',
            'task-create-technical-metadata.log'))\
            as open_file:
        assert open_file.read().startswith("Wrote METS technical metadata to file")

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id':'workspace'}
    )
    assert doc['_id'] == 'workspace'
    assert doc['wf_tasks']['create-technical-metadata']['messages']\
             == 'Technical metadata for objects created.'
    assert doc['wf_tasks']['create-technical-metadata']['result']\
            == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1


def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """

    (mongo_client, mongo_db, mongo_col) = mongo_settings()
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    mongodb_data = collection.find({"_id": document_id},
            {"wf_tasks.create-technical-metadata": 1})

    for item in mongodb_data:
        assert item["wf_tasks"]["create-technical-metadata"]["result"] == "success"

    # remove document from database
    collection.remove({"_id": document_id})

