"""Test ``siptools_research.workflow.send_sip`` module"""
import os
from siptools.scripts.compress import main
from siptools_research.workflow.send_sip import SendSIPToDP

def test_send_sip(testpath, testmongoclient):
    """Test the workflow task SendSip module.
    """

    # Force SendSIPToDP task to use SSH key from different path
    os.chmod('tests/data/pas_ssh_key', 0600)

    workspace = testpath
    create_sip = 'tests/data/testsip'
    #tar testsip
    sip_name = os.path.join(create_sip,
                            (os.path.basename(workspace) + '.tar'))
    main(['--tar_filename', sip_name, create_sip])

    # Init and run task
    task = SendSIPToDP(workspace=workspace,
                       sip_path=create_sip,
                       dataset_id='1',
                       config='tests/data/siptools_research.conf')
    task.run()
    assert task.complete()
    assert_mongodb_data_success(workspace)


def assert_mongodb_data_success(document_id):
    """Asserts that the task has written a successful outcome to
    MongoDB.
    """
