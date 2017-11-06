"""Test the `siptools_research.workflow.create_workspace` module"""

import os
import pymongo
from siptools_research.workflow import create_workspace

def test_createworkspace(testpath, testmongoclient):
    """Tests for `CreateWorkspace` task.

    - `Task.complete()` is true after `Task.run()`
    - Directory structure is created in workspace
    - Log entry is created to mongodb

    :testpath: Testpath fixture
    :testmongoclient: Pymongo mock fixture
    :returns: None
    """

    workspace = os.path.join(testpath, 'test_workspace')
    assert not os.path.isdir(workspace)

    # Init task
    task = create_workspace.CreateWorkspace(workspace=workspace,
                                            dataset_id="1")
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that directories were created
    assert os.path.isdir(workspace)
    assert os.path.isdir(os.path.join(workspace, 'logs'))
    assert os.path.isdir(os.path.join(workspace, 'sip-in-progress'))

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id': os.path.basename(workspace)}
    )
    assert doc['_id'] == os.path.basename(workspace)
    assert doc['workflow_tasks']['CreateWorkspace']['messages']\
        == 'Workspace directory created'
    assert doc['workflow_tasks']['CreateWorkspace']['result']\
        == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1
