"""Test the :mod:`siptools_research.create_sip.create_structmap` module"""

import os
import shutil
import getpass
from uuid import uuid4

import pytest

from tests.assertions import task_ok

from siptools_research.luigi.target import mongo_settings

from siptools_research.luigi.utils import UnknownReturnCode
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools_research.workflow.create_digiprov import CreateProvenanceInformation
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
#from siptools_research.create_sip.decompress import DecompressTransfer
#from siptools_research.create_sip.virus import ScanVirus
#from siptools_research.transfer.transfer import MoveTransferToWorkspace

from siptools_research.workflow import create_digiprov
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools.scripts import import_object
from siptools.scripts import import_description

def test_create_structmap_ok(testpath):
    """Test the workflow task CreateStructMap module.
       fixture testpath
    """
    testpath = './workspace'
    workspace = testpath
    print "!!! testp :%s " % testpath

     # Create workspace with "logs" and "sip-in-progress' directories in
    # temporary directory
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))
    #os.makedirs(os.path.join(workspace, 'sip-in-progress'))
    # Copy sample datacite.xml to workspace directory
    dmdpath = os.path.join(workspace, 'datacite.xml')
    shutil.copy('tests/data/datacite_sample.xml', dmdpath)
                #os.path.join(workspace, 'sip-in-progress', 'datacite.xml'))

    # Create dmdsec
    #task = CreateDescriptiveMetadata(home_path=workspace,
     #                                workspace=workspace)

    import_description.main([dmdpath, '--workspace', workspace])

     # Create provenance
    #os.makedirs(os.path.join(workspace, 'transfers'))
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace,  testfilename)#os.path.join(workspace, 'transfers', testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Init task
    task = create_digiprov.CreateProvenanceInformation(home_path=testpath,
                                                       workspace=workspace)


    # Create tech metadata
    test_data_folder = './tests/data/structured'

    import_object.main([test_data_folder, '--workspace', workspace])
    
    print "stuff "
    for lists in os.listdir(workspace): 
        path = os.path.join(workspace, lists) 
        print path 

    task = CreateStructMap(directory=test_data_folder, workspace=workspace)
    
    task.run()
    task.complete()
    print "stuff2 "
    for lists in os.listdir(workspace):
        path = os.path.join(workspace, lists)
        print path

    assert task_ok(task)
   # assert task.complete()
    assert os.path.isfile(os.path.join(workspace, 'filesec.xml'))
    assert os.path.isfile(os.path.join(workspace, 'structmap.xml'))

    #:assert_mongodb_data_success(os.listdir(workspace_root)[0])


def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """

    (mongo_client, mongo_db, mongo_col) = mongo_settings()
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    mongodb_data = collection.find({"_id": document_id},
            {"wf_tasks.create-structural-map-and-file-section.result": 1})

    for item in mongodb_data:
        assert item["wf_tasks"]["create-structural-map-and-file-section"]["result"] == "success"

    # remove document from database
    collection.remove({"_id": document_id})
