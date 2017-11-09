"""Test module ``siptools_research.workflow.create_mets``"""
import shutil
import os
from tests.assertions import task_ok
from siptools_research.luigi.target import mongo_settings
from siptools_research.workflow.create_mets import CreateMets
from siptools.scripts import import_object
from siptools.scripts import import_description, premis_event, \
    compile_structmap

def test_create_mets_ok(testpath):
    """Test the workflow task CreateMets module."""
    # Create workspace with contents required by the tested task
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    create_sip = os.path.join(workspace, 'sip_in_progress')
    os.makedirs(create_sip)
    create_test_data(workspace=create_sip)

    # Init and run task
    task = CreateMets(workspace=workspace, dataset_id='2')
    task.run()
    for directory in os.listdir(workspace):
        path = os.path.join(workspace, directory)
        print path
    assert task_ok(task)
    assert task.complete()
    assert os.path.isfile(os.path.join(create_sip, 'mets.xml'))

    assert_mongodb_data_success(workspace)


def create_test_data(workspace):
    """Create data needed to run ``CreateMets`` task"""

    # Copy sample datacite.xml to workspace directory
    dmdpath = os.path.join(workspace, 'datacite.xml')
    shutil.copy('tests/data/datacite_sample.xml', dmdpath)

    # Create dmdsec
    import_description.main([dmdpath, '--workspace', workspace])

     # Create provenance
    premis_event.main(['creation', '2016-10-13T12:30:55',
                       '--workspace', workspace,
                       '--event_outcome', 'success',
                       '--event_detail', 'Poika, 2.985 kg'])

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main([test_data_folder, '--workspace', workspace])

    # Create structmap
    compile_structmap.main(['--workspace', workspace])




def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """

    (mongo_client, mongo_db, mongo_col) = mongo_settings()
    mongodb = mongo_client[mongo_db]
    collection = mongodb[mongo_col]
    mongodb_data = collection.find(
        {"_id": document_id},
        {"wf_tasks.create-mets.result": 1})

    for item in mongodb_data:
        assert item["wf_tasks"]["create-mets"]["result"] == "success"

    # remove document from database
    collection.remove({"_id": document_id})
