"""Tests running parts of workflow"""

import pytest
import luigi.cmdline
import pymongo
from siptools_research.config import Configuration


def run_luigi_task(module, task, workspace):
    """Run luigi as it would be run from commandline"""
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module',
             module,
             task,
             '--workspace', workspace,
             '--dataset-id', 'workflow_test_dataset_1',
             '--config', 'tests/data/siptools_research.conf',
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
        # These tests do not work yet:
        # ('sign', 'SignSIP'),
        # ('compress', 'CompressSIP'),
        # ('send_sip', 'SendSIPToDP'),
        # ('report_preservation_status', 'ReportPreservationStatus'),
        # ('cleanup', 'CleanupWorkspace'),
    ]
)
def test_workflow(testpath, testmetax, testida, testmongoclient, module, task):
    """Run a task (and all tasks it requires) and check that check that report
    of successfull task is added to mongodb.
    """
    workspace = testpath
    run_luigi_task('siptools_research.workflow.' + module,
                   task,
                   workspace)

    conf = Configuration('tests/data/siptools_research.conf')
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()
    # Check 'result' field
    assert document['workflow_tasks'][task]['result'] == 'success'
