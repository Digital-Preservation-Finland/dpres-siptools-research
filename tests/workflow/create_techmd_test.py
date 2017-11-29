"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
import pymongo

from siptools_research.workflow.create_techmd import CreateTechnicalMetadata

def test_create_techmd_ok(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateTechnicalMetadata module.
    """
    # Create workspace with "logs" directory in temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))

    # Copy sample files to workspace directory "sip-in-progress" directory
    shutil.copytree('tests/data/files/',
                    os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="3",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML files are created in workspace
    assert os.path.isfile(os.path.join(
        workspace,
        'project_x_FROZEN%2Fsome%2Fpath%2Ffile_name_5-mets-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'project_x_FROZEN%2Fsome%2Fpath%2Ffile_name_6-mets-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'file_name_5-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'file_name_6-techmd.xml'
    ))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-technical-metadata.log')) as open_file:
        assert open_file.read().startswith(
            "Wrote METS technical metadata to file"
        )

    # Check that new log entry is found in mongodb, and that there is no extra
    # entries
    mongoclient = pymongo.MongoClient()
    doc = mongoclient['siptools-research'].workflow.find_one(
        {'_id':'workspace'}
    )
    assert doc['_id'] == 'workspace'
    assert doc['workflow_tasks']['CreateTechnicalMetadata']['messages']\
             == 'Technical metadata for objects created.'
    assert doc['workflow_tasks']['CreateTechnicalMetadata']['result']\
            == 'success'
    assert mongoclient['siptools-research'].workflow.count() == 1
