"""Each workflow task should be able complete if it is directly called by luigi
i.e. each task should know which other tasks are required to complete before it
itself can be run. This module tests that each task will succesfully run all
it's required tasks when it is called by luigi. Metax, Ida, mongodb,
paramiko.SSHClient and RemoteAnyTarget are mocked. Same sample dataset is used
for testing all tasks. It is only tested that each task will complete, the
output of task is NOT examined.
"""

import os
import importlib
from datetime import date

import pytest
import luigi
import pymongo
import mock
import lxml.etree as ET
from lxml.isoschematron import Schematron

from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.compress import CompressSIP
from siptools_research.config import Configuration
import tests.conftest


METS_XSD = "/etc/xml/dpres-xml-schemas/schema_catalogs/schemas/mets/mets.xsd"
CONTRACT = {
    "contract_json": {
        "title": "Testisopimus",
        "identifier": "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
        "organization": {
            "name": "Testiorganisaatio"
        }
    }
}
DIRECTORY = {
    "identifier": "pid:urn:dir:wf1",
    "directory_path": "/access"
}
SCHEMATRONS = [
    '/usr/share/dpres-xml-schemas/schematron/mets_addml.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_amdsec.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_audiomd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_digiprovmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_dmdsec.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_ead3.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_filesec.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_mdwrap.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_metshdr.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_mix.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_mods.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_premis_digiprovmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_premis_rightsmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_premis.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_premis_techmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_rightsmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_root.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_sourcemd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_structmap.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_techmd.sch',
    '/usr/share/dpres-xml-schemas/schematron/mets_videomd.sch'
]


def _init_files_col(mongoclient):
    """Init mocked upload.files collection"""
    mongo_files = [
        (
            "pid:urn:wf_test_1a_local",
            "tests/httpretty_data/ida/pid:urn:wf_test_1a_local"
        ),
        (
            "pid:urn:wf_test_1b_local",
            "tests/httpretty_data/ida/pid:urn:wf_test_1b_local"
        )
    ]
    for identifier, fpath in mongo_files:
        mongoclient.upload.files.insert_one(
            {"_id": identifier, "file_path": os.path.abspath(fpath)}
        )


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
@pytest.mark.parametrize("file_storage", ["ida", "local"])
@pytest.mark.usefixtures(
    'testmetax', 'testmongoclient', 'mock_luigi_config_path',
    'mock_filetype_conf', 'mock_metax_access'
)
def test_workflow(_, testpath, file_storage, module_name, task,
                  requests_mock):
    """Run a task (and all tasks it requires) and check that check that report
    of successfull task is added to mongodb.

    :param testpath: temporary directory
    :param file_storage: name of file source used in test ("ida" or "local")
    :param module_name: submodule of siptools_research.workflow that contains
                        Task to be tested
    :param task: Task class name
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get("https://metaksi/rest/v1/contracts/"
                      "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
                      json=CONTRACT)
    requests_mock.patch("https://metaksi/rest/v1/datasets/"
                        "workflow_test_dataset_1_local")
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json=DIRECTORY
    )
    with open("tests/data/datacite_sample.xml") as file_:
        requests_mock.get(
            "https://metaksi/rest/v1/datasets/workflow_test_dataset_1_local"
            "?dataset_format=datacite&dummy_doi=false",
            content=file_.read()
        )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1a_local/xml",
        json=[]
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1b_local/xml",
        json=[]
    )
    with open("tests/data/datacite_sample.xml") as file_:
        requests_mock.get(
            "https://metaksi/rest/v1/datasets/workflow_test_dataset_1_ida"
            "?dataset_format=datacite&dummy_doi=false",
            content=file_.read()
        )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1a_ida/xml",
        json=[]
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1b_ida/xml",
        json=[]
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:wf_test_1a_ida/download",
        content='foo'
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:wf_test_1b_ida/download",
        content='foo'
    )
    requests_mock.patch("https://metaksi/rest/v1/datasets/"
                        "workflow_test_dataset_1_ida")

    # Init pymongo client
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    database = mongoclient[conf.get('mongodb_database')]
    collection = database[conf.get('mongodb_collection')]

    dataset_id = 'workflow_test_dataset_1_%s' % file_storage
    # Add test identifiers to upload.files collection
    if file_storage == 'local':
        _init_files_col(mongoclient)

    with mock.patch.object(RemoteAnyTarget, '_exists', _mock_exists):
        workspace = os.path.join(testpath, 'workspace_' +
                                 os.path.basename(testpath))
        module = importlib.import_module('siptools_research.workflow.' +
                                         module_name)
        task_class = getattr(module, task)
        luigi.build(
            [task_class(
                workspace=workspace, dataset_id=dataset_id,
                config=tests.conftest.UNIT_TEST_CONFIG_FILE
            )],
            local_scheduler=True
        )

        document = collection.find_one()

        # Check 'result' field
        assert document['workflow_tasks'][task]['result'] == 'success'

    if module_name == "cleanup":
        assert document["completed"]


@pytest.mark.usefixtures(
    'testmetax', 'testmongoclient', 'mock_luigi_config_path',
    'mock_filetype_conf', 'mock_metax_access'
)
def test_mets_creation(testpath, requests_mock):
    """Run CompressSIP task (and all tasks it requires) and check that:

        #. report of successful task is added to mongodb.
        #. mets.xml validates against the schema
        #. mets.xml passes schematron verification
        #. digital object fixity (checksums) is correct in mets.xml
        #. digital objects of the SIP are valid
        #. mets.xml root element is valid (CONTRACTID, SPECIFICATION)

    :param testpath: temporary directory
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get("https://metaksi/rest/v1/contracts/"
                      "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
                      json=CONTRACT)
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:wf1",
        json=DIRECTORY
    )
    with open("tests/data/datacite_sample.xml") as file_:
        requests_mock.get(
            "https://metaksi/rest/v1/datasets/workflow_test_dataset_1_ida"
            "?dataset_format=datacite&dummy_doi=false",
            content=file_.read()
        )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1a_ida/xml",
        json=[]
    )
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:wf_test_1b_ida/xml",
        json=[]
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:wf_test_1a_ida/download",
        content='foo'
    )
    requests_mock.get(
        "https://ida.test/files/pid:urn:wf_test_1b_ida/download",
        content='foo'
    )
    requests_mock.patch("https://metaksi/rest/v1/datasets/"
                        "workflow_test_dataset_1_ida")

    luigi.build(
        [CompressSIP(
            workspace=testpath,
            dataset_id='workflow_test_dataset_1_ida',
            config=tests.conftest.UNIT_TEST_CONFIG_FILE
        )],
        local_scheduler=True
    )

    # Check 'result' field in database
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    document = (
        pymongo.MongoClient(host=conf.get('mongodb_host'))
        [conf.get('mongodb_database')]
        [conf.get('mongodb_collection')].find_one()
    )
    assert document['workflow_tasks']['CompressSIP']['result'] == 'success'

    # Read mets.xml
    mets = ET.parse(os.path.join(testpath, 'sip-in-progress', 'mets.xml'))

    # Validate mets.xml against schema
    schema = ET.XMLSchema(ET.parse(METS_XSD))
    assert schema.validate(mets)

    # Validate mets.xml against Schematrons
    for schematron in SCHEMATRONS:
        assert Schematron(ET.parse(schematron)).validate(mets)

    # Check mets root element
    mets_xml_root = mets.getroot()
    contractid = mets_xml_root.xpath('@*[local-name() = "CONTRACTID"]')[0]
    assert contractid == 'urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd'
    version = mets_xml_root.xpath('@*[local-name() = "CATALOG"] | '
                                  '@*[local-name() = "SPECIFICATION"]')[0][:3]
    assert version == '1.7'


def _mock_exists(_, path):
    if path.startswith('accepted/%s/' % date.today().strftime("%Y-%m-%d")):
        return True

    return False
