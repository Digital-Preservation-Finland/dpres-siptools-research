"""Test the `siptools_research.workflow.sign` module"""

import os
import shutil
import pymongo
from siptools_research.workflow import sign

def test_signsip(testpath, testmongoclient):
    """Tests for `SignSIP` task.

    - `Task.complete()` is true after `Task.run()`
    - Signature file created
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
    os.makedirs(os.path.join(workspace, 'logs'))
    sip_path = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sip_path)

    # Copy sample METS file to workspace
    shutil.copy(
        'tests/data/sample_mets.xml', os.path.join(sip_path, 'mets.xml')
    )

    # Init task
    task = sign.SignSIP(workspace=workspace,
                        dataset_id="1",
                        sign_key_path='tests/data/sign_keys')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that signature.sig is created in workspace/sip-in-progress/
    with open(os.path.join(workspace,
                           'sip-in-progress',
                           'signature.sig'))\
            as open_file:
        assert "This is an S/MIME signed message" in open_file.read()

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-sign-sip.log'))\
            as open_file:
        assert  open_file.read().startswith("sign_mets created file")

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id': os.path.basename(workspace)}
    )
    assert doc['_id'] == os.path.basename(workspace)
    assert doc['workflow_tasks']['SignSIP']['messages']\
        == 'Digital signature for SIP created.'
    assert doc['workflow_tasks']['SignSIP']['result']\
        == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1