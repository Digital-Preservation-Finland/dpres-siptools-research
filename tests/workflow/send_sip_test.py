import shutil
import getpass
import grp
import os

import pytest

from tests.assertions import task_ok

from siptools_research.luigi.target import mongo_settings
from siptools.scripts import import_object
from siptools.scripts import import_description, premis_event, compile_structmap
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools.scripts.compress import main

def test_send_sip(testpath):
    """Test the workflow task SendSip module.
    """
    workspace = testpath
    create_sip = 'tests/data/testsip' #os.path.join(workspace, "sip-in-progress")
    #tar testsip
    sip_name = os.path.join(create_sip,
                                (os.path.basename(workspace) + '.tar'))
    main(['--tar_filename', sip_name, create_sip])
 
    
    #test create mets task
    task = SendSIPToDP(workspace=workspace, sip_path='tests/data/testsip', dataset_id='1') 
    #SendSIPToDP(workspace=workspace, sip_output_path='tests/data/testsip', sip_creation_path=create_sip, home_path=workspace, dataset_id='1')
    task.run()
    assert task_ok(task)
    assert task.complete()
    assert_mongodb_data_success(workspace)




def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """


