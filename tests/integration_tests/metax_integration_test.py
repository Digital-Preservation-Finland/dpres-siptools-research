"""Test that the workflow can produce SIP using metax test server"""

import os
import pytest
import luigi.cmdline
import pymongo
from siptools_research.config import Configuration


def run_luigi_task(module, task, workspace):
    """Run luigi as it would be run from commandline"""
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', module, task,
             '--workspace', workspace,
             '--dataset-id', '616',
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler')
        )


# @pytest.mark.timeout(100)
# pylint: disable=unused-argument
def test_workflow(testpath, testmongoclient):
    """Run a task (and all tasks it requires) and check that check that report
    of successfull task is added to mongodb.
    """
    workspace = os.path.join(testpath, 'workspace_'+os.path.basename(testpath))
    run_luigi_task('siptools_research.workflow.create_mets',
                   'CreateMets',
                   workspace)


    # Init pymongo client
    conf = Configuration('tests/data/siptools_research.conf')
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks']['CreateMets']['result'] == 'success'
