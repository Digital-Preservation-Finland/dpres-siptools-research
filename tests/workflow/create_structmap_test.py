"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os
import shutil
import pymongo
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools_research.utils import database
from siptools.scripts import import_object
from siptools.scripts import import_description

def test_create_structmap_ok(testpath, testmongoclient):
    """Test the workflow task CreateStructMap module.
    """
    workspace = testpath
    sip_creation_path = os.path.join(workspace, "sip-in-progress")

    # Clean workspace and create "logs" directory in temporary directory
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))
    # Copy sample datacite.xml to workspace directory
    dmdpath = os.path.join(workspace, 'datacite.xml')
    shutil.copy('tests/data/datacite_sample.xml', dmdpath)

    # Create dmdsec
    import_description.main([dmdpath, '--workspace', sip_creation_path])

    # Create provenance
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Create tech metadata
    test_data_folder = './tests/data/structured'

    import_object.main([test_data_folder, '--workspace', sip_creation_path])

    # Create structmap
    task = CreateStructMap(workspace=workspace, dataset_id='1')

    task.run()
    assert task.complete()
    assert os.path.isfile(os.path.join(sip_creation_path, 'filesec.xml'))
    assert os.path.isfile(os.path.join(sip_creation_path, 'structmap.xml'))

    assert_mongodb_data_success(workspace)


def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """

    mongo_client = pymongo.MongoClient(database.HOST)
    mongo_db = database.DB
    mongo_col = database.COLLECTION
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    mongodb_data = collection.find(
        {"_id": document_id},
        {"wf_tasks.create-structural-map-and-file-section.result": 1})

    for item in mongodb_data:
        assert item["wf_tasks"]["create-structural-map-and-file-section"]\
            ["result"] == "success"

    # remove document from database
    collection.remove({"_id": document_id})
