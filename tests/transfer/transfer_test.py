"""Test the :mod:`preservation.transfer.transfer` module"""

import os
from tempfile import NamedTemporaryFile

import getpass

from tests.assertions import task_ok

#import preservation.premis as p
#import preservation.taskreport as tr

from siptools_research.target import mongo_settings

from siptools_research.transfer.transfer import MoveTransferToWorkspace
from siptools_research.transfer.transfer import ReadyForTransfer
#from preservation.target import TaskReportTarget


def test_move_transfer_to_workspace(testpath, packagefile):
    """Test the :mod:`preservation.transfer.transfer` module /
    MoveTransferToWorkspace task.

    :testpath: Testpath fixture
    :packagefile: packagefile fixture
    :returns: None

    """

    sip = packagefile()

    workspace_root = testpath.workspace_root

    # Move transfer to workspace

    task = MoveTransferToWorkspace(
        filename=sip["filename"],
        workspace_root=workspace_root,
        home_path=testpath.workspace_root,
        min_age=0,
        username=getpass.getuser())

    assert task_ok(task)
    assert task.complete()

    assert_mongodb_document(os.listdir(workspace_root)[0])


def test_ready_for_transfer_complete():
    with NamedTemporaryFile() as temp:
        temp.write('Some data')
        temp.flush()
        task = ReadyForTransfer(filename=temp.name, min_age=0)
        assert task_ok(task)
        assert task.complete()


def test_ready_for_transfer_incomplete():
    with NamedTemporaryFile() as temp:
        temp.write('Some data')
        temp.flush()
        task = ReadyForTransfer(filename=temp.name, min_age=5)
        assert task_ok(task)
        assert not task.complete()


def assert_mongodb_document(document_id):
    """Asserts that MoveTransferToWorkSpace has created a document in
    MongoDB with the correct id.
    """
    (mongo_client, mongo_db, mongo_col) = mongo_settings()
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    count = collection.count({"_id": document_id})

    assert count == 1
    
    # remove document from database
    collection.remove({"_id": document_id})
