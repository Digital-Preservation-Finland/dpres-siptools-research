"""Tests for packaging workflow.

Each workflow task should be able complete if it is directly called
by luigi i.e. each task should know which other tasks are required to
complete before it itself can be run. This module tests that each task
will succesfully run all it's required tasks when it is called by luigi.
Metax, Ida, mongodb, paramiko.SSHClient and RemoteAnyTarget are mocked.
Same sample dataset is used for testing all tasks. It is only tested
that each task will complete, the output of task is NOT examined.
"""

import datetime
import importlib

from unittest import mock

import pytest
import luigi
import pymongo

from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.config import Configuration
import tests.metax_data.contracts
import tests.utils


# Run every task as it would be run from commandline
@mock.patch('siptools_research.workflow.send_sip.paramiko.SSHClient',
            new=mock.MagicMock)
@pytest.mark.parametrize(
    "module_name,task", [
        ('create_workspace', 'CreateWorkspace'),
        ('validate_metadata', 'ValidateMetadata'),
        ('create_digiprov', 'CreateProvenanceInformation'),
        ('create_dmdsec', 'CreateDescriptiveMetadata'),
        ('get_files', 'GetFiles'),
        ('create_techmd', 'CreateTechnicalMetadata'),
        ('create_structmap', 'CreateStructMap'),
        ('create_logical_structmap', 'CreateLogicalStructMap'),
        ('create_mets', 'CreateMets'),
        ('sign', 'SignSIP'),
        ('compress', 'CompressSIP'),
        ('send_sip', 'SendSIPToDP'),
        ('report_preservation_status', 'ReportPreservationStatus'),
        ('cleanup', 'CleanupWorkspace'),
    ]
)
@pytest.mark.usefixtures(
    'testmongoclient', 'mock_luigi_config_path', 'mock_filetype_conf'
)
def test_workflow(pkg_root, module_name, task, requests_mock):
    """Test workflow dependency tree.

    Run a task (and all tasks it requires) and check that report of
    successful task is added to mongodb.

    :param pkg_root: temporary packaging root directory
    :param module_name: submodule of siptools_research.workflow that
                        contains Task to be tested
    :param task: Task class name
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    tests.utils.add_metax_dataset(requests_mock,
                                  tests.metax_data.datasets.BASE_DATASET,
                                  files=[tests.metax_data.files.TXT_FILE])
    tests.utils.add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id="dataset_identifier",
        filename="path/to/file",
        content=b"foo"
    )

    # Init pymongo client
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))

    with mock.patch.object(RemoteAnyTarget, '_exists', _mock_exists):
        workspace = str(pkg_root / 'workspaces' / 'workspace')
        module = importlib.import_module('siptools_research.workflow.'
                                         + module_name)
        task_class = getattr(module, task)
        luigi.build(
            [task_class(
                workspace=workspace,
                dataset_id='dataset_identifier',
                config=tests.conftest.UNIT_TEST_CONFIG_FILE
            )],
            local_scheduler=True
        )

    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks'][task]['result'] == 'success'

    if module_name == "cleanup":
        assert document["completed"]


def _mock_exists(_, path):
    return 'accepted/%s/' % datetime.date.today().strftime("%Y-%m-%d") in path
