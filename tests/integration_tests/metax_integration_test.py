"""Test that the workflow can produce SIP using Metax test server. This test
requires writing dataset and file metadata to Metax. Therefore the password for
Metax user 'tpas' is prompted during the test."""

import os
import json
import getpass
import random
import pytest
import luigi.cmdline
import pymongo
import requests
import requests_mock
from siptools_research.config import Configuration


def run_luigi_task(module, task, workspace, dataset_id):
    """Run luigi as it would be run from commandline"""
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', module, task,
             '--workspace', workspace,
             '--dataset-id', dataset_id,
             '--config', pytest.TEST_CONFIG_FILE,
             '--local-scheduler')
        )


def test_workflow(testpath, testmongoclient):
    """Add test dataset metadata and associated file metadata to Metax. Run
    partial workflow by calling CreateMets task with luigi.
    """
    # Read configuration file
    conf = Configuration(pytest.TEST_CONFIG_FILE)
    # Override Metax password in configuration file with real password from
    # the user
    # pylint: disable=protected-access
    conf._parser.set(
        'siptools_research', 'metax_password',
        getpass.getpass(prompt='Metax password for user \'tpas\':')
    )

    # Post files to Metax
    file1_metadata = 'tests/data/integration_tests/metax_file_1.json'
    file2_metadata = 'tests/data/integration_tests/metax_file_2.json'
    file1_id = 'tpas_test_'+ str(random.randint(1, 1000000))
    file2_id = 'tpas_test_'+ str(random.randint(1, 1000000))
    post_metax_file(file1_id, file1_metadata, conf)
    post_metax_file(file2_id, file2_metadata, conf)

    # Post dataset to Metax
    dataset_metadata = 'tests/data/integration_tests/metax_dataset.json'
    dataset_id = post_metax_dataset(dataset_metadata,
                                    [file1_id, file2_id],
                                    conf)

    with requests_mock.Mocker(real_http=True) as ida_mock:
        # Mock Ida
        ida_mock.get("https://86.50.169.61:4433/files/%s/download" % file1_id,
                     text='adsf')
        ida_mock.get("https://86.50.169.61:4433/files/%s/download" % file2_id,
                     text='adsf')

        # Run partial workflow for dataset just added to Metax
        workspace = os.path.join(testpath,
                                 'workspace_'+os.path.basename(testpath))
        run_luigi_task('siptools_research.workflow.create_mets',
                       'CreateMets',
                       workspace,
                       str(dataset_id))

    # Init pymongo client
    conf = Configuration(pytest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks']['CreateMets']['result'] == 'success'


def post_metax_file(identifier, metadatafile, conf):
    """Post file metadata to Metax using HTTP POST method.

    :identifier: unique identifier for file
    :metadatafile: JSON file from which the metadata is read
    :returns: None
    """
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


def post_metax_dataset(metadatafile, file_ids, conf):
    """Post dataset metadata to Metax using HTTP POST method.

    :identifier: unique identifier for file
    :metadatafile: JSON file from which the metadata is read
    :returns: Id of added dataset
    """
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
