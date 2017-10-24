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

def test_send_sip(testpath):
    """Test the workflow task SendSip module.
    """
    workspace = testpath
   

    #test create mets task
    task = SendSIPToDP(workspace=workspace, sip_output_path='tests/data/testsip', sip_creation_path=workspace, home_path=workspace, dataset_id='1')
    task.run()
    assert task_ok(task)
    assert task.complete()
    assert_mongodb_data_success(workspace)




def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """


