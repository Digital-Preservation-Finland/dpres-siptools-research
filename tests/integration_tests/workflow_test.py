"""Each workflow task should be able complete if it is directly called by luigi
i.e. each task should know which other tasks are required to complete before it
itself can be run. This module tests that each task will succesfully run all
it's required tasks when it is called by luigi. Metax, Ida, mongodb,
paramiko.SSHClient and RemoteAnyTarget are mocked. Same sample dataset is used
for testing all tasks. It is only tested that each task will complete, the
output of task is NOT examined.
"""

import os
import pytest
import tests.conftest
import luigi
import pymongo
from siptools_research.config import Configuration
import mock
from siptools_research.remoteanytarget import RemoteAnyTarget
import importlib
from ipt.scripts import (check_xml_schema_features,
                         check_xml_schematron_features,
                         check_sip_file_checksums,
                         check_sip_digital_objects)
from siptools_research.workflow.compress import CompressSIP
import uuid
import lxml.etree as ET


# Run every task as it would be run from commandline
@mock.patch('siptools_research.workflow.send_sip.paramiko.SSHClient')
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
@pytest.mark.usefixtures('testmetax', 'testida', 'testmongoclient',
                         'mock_luigi_config_path', 'mock_filetype_conf')
@pytest.mark.timeout(600)
def test_workflow(_mock_ssh, testpath, module_name, task):
    """Run a task (and all tasks it requires) and check that check that report
    of successfull task is added to mongodb.

    :param _mock_ssh: mocked paramiko.SSHClient
    :param testpath: temporary directory
    :param module_name: submodule of siptools_research.workflow that contains
                    Task to be tested
    :param task: Task class name
    :returns: ``None``
    """
    with mock.patch.object(RemoteAnyTarget, '_exists', mock_exists):
        workspace = os.path.join(testpath, 'workspace_' +
                                 os.path.basename(testpath))
        module = importlib.import_module('siptools_research.workflow.' +
                                         module_name)
        task_class = getattr(module, task)
        luigi.build([task_class(workspace=workspace,
                                dataset_id='workflow_test_dataset_1',
                                config=tests.conftest.UNIT_TEST_CONFIG_FILE)],
                    local_scheduler=True)

        # Init pymongo client
        conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
        mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
        collection = (mongoclient[conf.get('mongodb_database')]
                      [conf.get('mongodb_collection')])
        document = collection.find_one()

        # Check 'result' field
        assert document['workflow_tasks'][task]['result'] == 'success'


@pytest.mark.usefixtures('testmetax', 'testida', 'testmongoclient',
                         'mock_luigi_config_path', 'mock_filetype_conf')
def test_mets_creation(testpath):
    """Run CompressSIP task (and all tasks it requires) and check that:

        #. report of successful task is added to mongodb.
        #. mets.xml validates against the schema
        #. mets.xml passes schematron verification
        #. digital object fixity (checksums) is correct in mets.xml
        #. digital objects of the SIP are valid
        #. mets.xml root element is valid (CONTRACTID, SPECIFICATION)

    :param testpath: temporary directory
    :returns: ``None``
    """

    workspace = os.path.join(testpath,
                             'workspace_' + os.path.basename(testpath))
    luigi.build([CompressSIP(workspace=workspace,
                             dataset_id='workflow_test_dataset_1',
                             config=tests.conftest.UNIT_TEST_CONFIG_FILE)],
                local_scheduler=True)
    # Init pymongo client
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks']['CompressSIP']['result'] == 'success'
    mets_path = os.path.join(workspace, 'sip-in-progress', 'mets.xml')
    assert check_xml_schema_features.main([mets_path]) == 0
    assert_schematron(mets_path)
    assert check_sip_file_checksums.main([mets_path]) == 0
    assert check_sip_digital_objects.main([mets_path, 'preservation-sip-id',
                                           str(uuid.uuid4())]) == 0
    assert_mets_root_element(mets_path)


def assert_schematron(mets_path):
    schematron_path = '/usr/share/dpres-xml-schemas/schematron'
    schematron_rules = [
        'mets_addml.sch',
        'mets_amdsec.sch',
        'mets_audiomd.sch',
        'mets_digiprovmd.sch',
        'mets_dmdsec.sch',
        'mets_ead3.sch',
        'mets_filesec.sch',
        'mets_mdwrap.sch',
        'mets_metshdr.sch',
        'mets_mix.sch',
        'mets_mods.sch',
        'mets_premis_digiprovmd.sch',
        'mets_premis_rightsmd.sch',
        'mets_premis.sch',
        'mets_premis_techmd.sch',
        'mets_rightsmd.sch',
        'mets_root.sch',
        'mets_sourcemd.sch',
        'mets_structmap.sch',
        'mets_techmd.sch',
        'mets_videomd.sch']
    for rule in schematron_rules:
        rule_path = os.path.join(schematron_path, rule)
        assert check_xml_schematron_features.main(['-s',
                                                   rule_path, mets_path]) == 0


def assert_mets_root_element(mets_path):
    mets_xml_root = ET.parse(mets_path).getroot()
    contractid = mets_xml_root.xpath('@*[local-name() = "CONTRACTID"]')[0]
    assert contractid == 'urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd'
    version = mets_xml_root.xpath('@*[local-name() = "CATALOG"] | '
                                  '@*[local-name() = "SPECIFICATION"]')[0][:3]
    assert version == '1.7'


def mock_exists(self, path):
    if path.startswith('accepted'):
        return True
    return False
