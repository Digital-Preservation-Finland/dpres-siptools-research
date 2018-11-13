"""Each workflow task should be able complete if it is directly called by luigi
i.e. each task should know which other tasks are required to complete before it
itself can be run. This module tests that each task will succesfully run all
it's required tasks when it is called by luigi. Metax, Ida, and mongodb are
mocked. Same sample dataset is used for testing all tasks. It is only tested
that each task willl complete, the output of task is NOT examined.
"""

import os
import pytest
import tests.conftest
import luigi.cmdline
import pymongo
from siptools_research.config import Configuration


def run_luigi_task(module, task, workspace):

    """Run any WorkflowTask using luigi as it would be run from commandline
    :param module: full path module that contains the Task
    :param task: Task class name
    :param workspace: --workspace parameter for WorkflowTask
    :returns: ``None``
    """
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', module, task,
             '--workspace', workspace,
             '--dataset-id', 'workflow_test_dataset_1',
             '--config', tests.conftest.UNIT_TEST_CONFIG_FILE,
             '--worker-keep-alive',
             '--worker-retry-external-tasks',
             '--scheduler-retry-delay', '20',
             '--local-scheduler',
             '--no-lock')
        )


# Run every task as it would be run from commandline
@pytest.mark.parametrize(
    "module,task", [
        ('create_workspace', 'CreateWorkspace'),
        ('validate_metadata', 'ValidateMetadata'),
        ('create_digiprov', 'CreateProvenanceInformation'),
        ('create_dmdsec', 'CreateDescriptiveMetadata'),
        ('get_files', 'GetFiles'),
        ('create_techmd', 'CreateTechnicalMetadata'),
        ('create_structmap', 'CreateStructMap'),
        ('create_mets', 'CreateMets'),
        ('sign', 'SignSIP'),
        ('compress', 'CompressSIP'),
        ('send_sip', 'SendSIPToDP'),
        ('report_preservation_status', 'ReportPreservationStatus'),
        ('cleanup', 'CleanupWorkspace'),
    ]
)
@pytest.mark.usefixtures('testmetax', 'testida', 'testmongoclient',
                         'mock_luigi_config_path', 'mock_filetype_conf')
@pytest.mark.timeout(600)
def test_workflow(testpath, module, task):
    """Run a task (and all tasks it requires) and check that check that report
    of successfull task is added to mongodb.

    :param testpath: temporary directory
    :param module: submodule of siptools_research.workflow that contains Task
                   to be tested
    :param task: Task class name
    :returns: ``None``
    """
    # Set permissions of ssh key (required by SendSIPToDP task)
    os.chmod('tests/data/pas_ssh_key', 0600)

    workspace = os.path.join(testpath, 'workspace_'+os.path.basename(testpath))
    run_luigi_task('siptools_research.workflow.' + module,
                   task,
                   workspace)

    # Init pymongo client
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks'][task]['result'] == 'success'
