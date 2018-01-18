"""Test that the workflow can produce SIP using metax test server"""

import os
import json
import random
import pytest
import luigi.cmdline
import pymongo
import requests
from siptools_research.config import Configuration


def run_luigi_task(module, task, workspace, dataset_id):
    """Run luigi as it would be run from commandline"""
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', module, task,
             '--workspace', workspace,
             '--dataset-id', dataset_id,
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler')
        )


def test_workflow(testpath, testmongoclient, testida):
    """Add test dataset metadata and associated file metadata to Metax. Run
    partial workflow by calling CreateMets task with luigi.
    """
    # Post files to Metax
    file1_metadata = 'tests/data/integration_tests/metax_file_1.json'
    file2_metadata = 'tests/data/integration_tests/metax_file_2.json'
    file1_id = 'tpas_test_'+ str(random.randint(1, 1000000))
    file2_id = 'tpas_test_'+ str(random.randint(1, 1000000))
    post_metax_file(file1_id, file1_metadata)
    post_metax_file(file2_id, file2_metadata)

    # Post dataset to Metax
    dataset_metadata = 'tests/data/integration_tests/metax_dataset.json'
    dataset_id = post_metax_dataset(dataset_metadata, [file1_id, file2_id])

    # Run partial workflow for dataset just added to Metax
    workspace = os.path.join(testpath, 'workspace_'+os.path.basename(testpath))
    run_luigi_task('siptools_research.workflow.create_mets',
                   'CreateMets',
                   workspace,
                   dataset_id)

    # Init pymongo client
    conf = Configuration('tests/data/siptools_research.conf')
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks']['CreateMets']['result'] == 'success'


def post_metax_file(identifier, metadatafile):
    """Post file metadata to Metax using HTTP POST method.

    :identifier: unique identifier for file
    :metadatafile: JSON file from which the metadata is read
    :returns: None
    """
    # Read Metax configuration
    conf = Configuration('siptools_research.conf')

    # Replace file identifier in metadata with identifier given as parameter
    with open(metadatafile) as open_file:
        data = json.load(open_file)
    data["identifier"] = identifier

    # Post metadata
    url = "%s/rest/v1/files/" % conf.get("metax_url")
    response = requests.post(
        url, json=data,
        auth=(conf.get("metax_user"), conf.get("metax_password"))
    )
    assert response.status_code == 201


def post_metax_dataset(metadatafile, file_ids):
    """Post dataset metadata to Metax using HTTP POST method.

    :identifier: unique identifier for file
    :metadatafile: JSON file from which the metadata is read
    :returns: Id of added dataset
    """
    # Read Metax configuration
    conf = Configuration('siptools_research.conf')

    # Edit metadata
    with open(metadatafile) as open_file:
        data = json.load(open_file)

    # Replace file identifiers in metadata with strings given as parameter
    fnum = 0
    for file_id in file_ids:
        data["research_dataset"]["files"][fnum]["identifier"] = file_id
        fnum += 1

    # Post metadata
    url = "%s/rest/v1/datasets/" % conf.get("metax_url")
    response = requests.post(
        url, json=data,
        auth=(conf.get("metax_user"), conf.get("metax_password"))
    )
    assert response.status_code == 201

    return response.json()['id']
