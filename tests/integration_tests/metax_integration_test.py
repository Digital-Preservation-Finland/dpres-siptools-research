"""Test that the workflow can produce SIP using Metax test server. This test
requires writing dataset and file metadata to Metax. Therefore the password for
Metax user 'tpas' is prompted during the test."""

import os
import json
import getpass
import pytest
import tests.conftest
import luigi.cmdline
import pymongo
import requests
import requests_mock

from siptools_research.config import Configuration


try:
    from configparser import ConfigParser
except ImportError:  # Python 2
    from ConfigParser import ConfigParser


def run_luigi_task(module, task, workspace, dataset_id):
    """Run any WorkflowTask with luigi as it would be run from commandline.

    :param module: full path module that contains task
    :param task: name of task class
    :param workspace: --workspace parameter for WorkflowTask
    :param dataset_id: --dataset-id parameter for WorkflowTask
    :returns: ``None``
    """
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', module, task,
             '--workspace', workspace,
             '--dataset-id', dataset_id,
             '--config', tests.conftest.TEST_CONFIG_FILE,
             '--local-scheduler')
        )


def get_metax_password():
    """
    Retrieve the Metax password, trying first to read from a local
    configuration file and then using a password prompt
    """
    try:
        config = ConfigParser()
        config.read(os.path.expanduser("~/.metax.cfg"))
        if config["metax"]["user"] == "tpas":
            return config["metax"]["password"]
    except KeyError:
        # Config file does not exist
        pass

    # Fall back to a password prompt
    return getpass.getpass(prompt='Metax password for user \'tpas\':')


@pytest.mark.usefixtures(
    'testmongoclient', 'mock_luigi_config_path', 'mock_filetype_conf'
)
def test_workflow(testpath):
    """Add test dataset metadata and associated file metadata to Metax. Run
    partial workflow by calling CreateMets task with luigi.

    :param testpath: temporary directory
    :returns: ``None``
    """
    dataset_id = -1
    file1_id = "pid:urn:wf_test_1a"
    file2_id = "pid:urn:wf_test_1b"
    try:
        # Read configuration file
        conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
        # Override Metax password in configuration file with real password from
        # the user
        # pylint: disable=protected-access
        conf._parser.set(
            'siptools_research', 'metax_password',
            get_metax_password()
        )

        # Post files to Metax
        file1_metadata = 'tests/data/integration_tests/metax_file_1.json'
        file2_metadata = 'tests/data/integration_tests/metax_file_2.json'
        post_metax_file(file1_metadata, conf)
        post_metax_file(file2_metadata, conf)

        # Post dataset to Metax
        dataset_metadata = 'tests/data/integration_tests/metax_dataset.json'
        dataset_id = post_metax_dataset(dataset_metadata,
                                        [file1_id, file2_id],
                                        conf)

        with requests_mock.Mocker(real_http=True) as ida_mock:
            # Mock Ida
            ida_mock.get(conf.get('ida_url') + "/files/%s/download" % file1_id,
                         text='adsf')
            ida_mock.get(conf.get('ida_url') + "/files/%s/download" % file2_id,
                         text='adsf')

            # Run partial workflow for dataset just added to Metax
            workspace = str(testpath / f'workspace_{testpath.name}')
            run_luigi_task('siptools_research.workflow.create_mets',
                           'CreateMets',
                           workspace,
                           str(dataset_id))

        # Init pymongo client
        conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
        mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
        collection = (mongoclient[conf.get('mongodb_database')]
                      [conf.get('mongodb_collection')])
        document = collection.find_one()

        # Check 'result' field
        assert document['workflow_tasks']['CreateMets']['result'] == 'success'
    finally:
        delete_metax_file(file1_id, conf)
        delete_metax_file(file2_id, conf)
        delete_metax_dataset(dataset_id, conf)


def post_metax_file(metadatafile, conf):
    """Post file metadata to Metax using HTTP POST method.

    :metadatafile: JSON file from which the metadata is read
    :param conf: Configuration
    :returns: ``None``
    """
    # Read metadata file
    with open(metadatafile) as open_file:
        data = json.load(open_file)

    # Post metadata
    url = "%s/rest/v1/files/" % conf.get("metax_url")
    response = requests.post(
        url, json=data,
        auth=(conf.get("metax_user"), conf.get("metax_password"))
    )
    assert response.status_code == 201


def post_metax_dataset(metadatafile, file_ids, conf):
    """Post dataset metadata to Metax using HTTP POST method.

    :param identifier: unique identifier for file
    :param metadatafile: JSON file from which the metadata is read
    :param conf: Configuration
    :returns: Id of added dataset
    """
    auth = (conf.get("metax_user"), conf.get("metax_password"))

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
        auth=auth
    )
    assert response.status_code == 201

    identifier = response.json()['identifier']

    # Add preservation identifier
    response = requests.post(
        "{}/rpc/datasets/set_preservation_identifier".format(
            conf.get("metax_url")
        ),
        params={"identifier": identifier},
        auth=auth
    )
    assert response.status_code == 200

    return identifier


def delete_metax_file(identifier, conf):
    """Delete file metadata from Metax using HTTP DELETE method.

    :param identifier: Identifier of file to be deleted
    :param conf: Configuration
    :returns: None
    """
    url = "%s/rest/v1/files/%s" % (conf.get("metax_url"), identifier)
    response = requests.delete(
        url,
        auth=(conf.get("metax_user"), conf.get("metax_password"))
    )
    assert response.status_code == 200 or response.status_code == 404


def delete_metax_dataset(identifier, conf):
    """Delete dataset metadata from Metax using HTTP DELETE method.

    :param identifier: Identifier of dataset to be deleted
    :param conf: Configuration
    :returns: ``None``
    """

    url = "%s/rest/v1/datasets/%s" % (conf.get("metax_url"), identifier)
    response = requests.delete(
        url,
        auth=(conf.get("metax_user"), conf.get("metax_password"))
    )
    assert response.status_code == 204 or response.status_code == 404
