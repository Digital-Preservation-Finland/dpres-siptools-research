"""Test the :mod:`siptools_research.create_sip.create_digiprov` module"""

import os
import shutil
import getpass
from uuid import uuid4

import pytest

from tests.assertions import task_ok

from siptools_research.target import mongo_settings

from siptools_research.workflow.utils import UnknownReturnCode
from siptools_research.create_sip.create_digiprov import CreateProvenanceInformation
from siptools_research.create_sip.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.create_sip.signature import ValidateDigitalSignature
from siptools_research.create_sip.create_ead3 import get_nativeid, CreateEAD3
from siptools_research.create_sip.decompress import DecompressTransfer
from siptools_research.create_sip.virus import ScanVirus
from siptools_research.transfer.transfer import MoveTransferToWorkspace


def test_create_digiprov_ok(testpath, tarfile):
    """Test the workflow task CreateProvenanceInformation module.
    """

    sip = tarfile()

    workspace_root = testpath.workspace

    transfer = MoveTransferToWorkspace(
        filename=sip["filename"],
        workspace_root=workspace_root,
        home_path=testpath.workspace_root,
        min_age=0,
        username=getpass.getuser())

    assert task_ok(transfer)

    workspace = os.path.join(workspace_root, os.listdir(workspace_root)[0])
    sip_creation_path = os.path.join(workspace, 'sip-in-progress')

    decompress = DecompressTransfer(workspace=workspace,
                                    sip_creation_path=sip_creation_path,
                                    home_path='/home')

    task_ok(decompress)

    # skip the virus check task by creating output file
    shutil.copy(os.path.join(workspace, 'task-output-files', 'decompress'),
            os.path.join(workspace, 'task-output-files', 'virus'))

    # skip the signature check task by creating output file
    shutil.copy(os.path.join(workspace, 'task-output-files', 'decompress'),
            os.path.join(workspace, 'task-output-files',
                'validation-digital-signature'))

    create_ead3 = CreateEAD3(workspace=workspace,
            sip_creation_path=sip_creation_path,
                home_path='/home')

    task_ok(create_ead3)

    create_dmdsec = CreateDescriptiveMetadata(workspace=workspace,
            sip_creation_path=sip_creation_path,
                home_path='/home')

    task_ok(create_dmdsec)

    task = CreateProvenanceInformation(workspace=workspace,
                    sip_creation_path=sip_creation_path,
                                    home_path='/home')

    assert task_ok(task)
    assert task.complete()
    assert os.path.isfile(os.path.join(sip_creation_path, 'migration-event.xml'))
    assert os.path.isfile(os.path.join(sip_creation_path, 'migration-agent.xml'))

    assert_mongodb_data_success(os.listdir(workspace_root)[0])


def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """

    (mongo_client, mongo_db, mongo_col) = mongo_settings()
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    mongodb_data = collection.find({"_id": document_id},
            {"wf_tasks.create-provenance-information.result": 1})

    for item in mongodb_data:
        assert item["wf_tasks"]["create-provenance-information"]["result"] == "success"

    # remove document from database
    collection.remove({"_id": document_id})

