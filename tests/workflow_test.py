"""Unit tests for :mod:`siptools_research.workflow` package."""
import copy
import filecmp
import importlib
import shutil
import tarfile
from datetime import datetime, timedelta

import luigi
import lxml.etree
import pymongo
import pytest
from lxml.isoschematron import Schematron
from upload_rest_api.models.file_entry import FileEntry

import siptools_research.workflow.compress
import siptools_research.workflow.create_mets
import tests.metax_data.reference_data
import tests.utils
from siptools_research.config import Configuration
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import (
    AUDIO_FILE,
    CSV_FILE,
    MKV_FILE,
    PAS_STORAGE_SERVICE,
    PDF_FILE,
    SEG_Y_FILE,
    TIFF_FILE,
    TXT_FILE,
    VIDEO_FILE,
)

METS_XSD = "/etc/xml/dpres-xml-schemas/schema_catalogs/schemas/mets/mets.xsd"
PAS_STORAGE_TXT_FILE = copy.deepcopy(TXT_FILE)
PAS_STORAGE_TXT_FILE["storage_service"] = PAS_STORAGE_SERVICE
METS_FILE = copy.deepcopy(TXT_FILE)
METS_FILE["pathname"] = "mets.xml"
SIG_FILE = copy.deepcopy(TXT_FILE)
SIG_FILE["pathname"] = "signature.sig"

SCHEMATRONS = []
SCHEMATRON_FILES = [
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

NAMESPACES = {
    "mets": "http://www.loc.gov/METS/",
    "premis": "info:lc/xmlns/premis-v2",
}


def _get_schematrons():
    """Get cached Schematrons.

    Parsing Schematron files is slow, so they are parsed only when this
    function is called first time.
    """
    if not SCHEMATRONS:
        for schematron_file in SCHEMATRON_FILES:
            schematron = Schematron(lxml.etree.parse(schematron_file))
            SCHEMATRONS.append(schematron)

    return SCHEMATRONS


@pytest.mark.parametrize(
    ("module_name", "task"),
    [
        ("cleanup", "Cleanup"),
        ("compress", "CompressSIP"),
        ("copy_dataset_to_pas_data_catalog", "CopyToPasDataCatalog"),
        ("create_mets", "CreateMets"),
        ("generate_metadata", "GenerateMetadata"),
        ("get_files", "GetFiles"),
        ("report_dataset_validation_result", "ReportDatasetValidationResult"),
        ("report_preservation_status", "ReportPreservationStatus"),
        ("send_sip", "SendSIPToDP"),
        ("sign", "SignSIP"),
        ("validate_files", "ValidateFiles"),
        ("validate_metadata", "ValidateMetadata"),
        #("poll_reports", "GetValidationReports"),
    ]
)
@pytest.mark.usefixtures("testmongoclient", "mock_luigi_config_path")
def test_workflow(workspace, module_name, task, requests_mock, mock_ssh_config,
                  sftp_dir):
    """Test workflow dependency tree.

    Each workflow task should be able complete if it is directly called
    by luigi i.e. each task should know which other tasks are required
    to complete before it itself can be run. This test runs a task (and
    all tasks it requires) and checks that report of successful task is
    added to mongodb. The output of task is NOT examined. Metax, Ida,
    mongodb, paramiko.SSHClient and RemoteAnyTarget are mocked.

    :param workspace: temporary workspace directory
    :param module_name: submodule of siptools_research.workflow that
                        contains Task to be tested
    :param task: Task class name
    :param requests_mock: HTTP request mocker
    """
    # Create transfer directory to PAS
    (sftp_dir / "transfer").mkdir(parents=True, exist_ok=True)

    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = workspace.name
    dataset["persistent_identifier"] = "doi:test"
    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)
    requests_mock.post("/v3/files/patch-many")

    # Mock Ida
    tests.utils.add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="/path/to/file",
        content=b"foo\n"
    )

    # Mock DPS
    requests_mock.get(
        "https://access.localhost/api/2.0/urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd/ingest/report/doi%3Atest",
        json={
            "data": {
                "results": [
                    {
                        "download": {
                            "html": "foo?type=html",
                            "xml": "foo?type=xml"
                        },
                        "id": "doi:test",
                        "date": (datetime.now()+timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "status": "accepted"
                    }
                ]
            }
        }
    )
    requests_mock.get('https://access.localhost/api/2.0/urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd/ingest/report/doi%3Atest/doi%3Atest?type=xml',
                      content=b'<hello world/>')
    requests_mock.get('https://access.localhost/api/2.0/urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd/ingest/report/doi%3Atest/doi%3Atest?type=html',
                      content=b'<hello world/>')


    # Init pymongo client
    conf = Configuration(mock_ssh_config)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))

    module = importlib.import_module('siptools_research.workflow.'
                                     + module_name)
    task_class = getattr(module, task)

    # The workflow is run twice because GetValidationReports in poll_reports.py
    #  is an external task and triggers only with the second run.
    luigi.build(
        [task_class(
            dataset_id=workspace.name,
            config=str(mock_ssh_config)
        )],
        local_scheduler=True
    )
    luigi.build(
        [task_class(
            dataset_id=workspace.name,
            config=str(mock_ssh_config)
        )],
        local_scheduler=True
    )

    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks'][task]['result'] == 'success'

    # The workspace root directory should contain only metadata
    # generation workspace, validation workspace, preservation
    # workspaces and file cache.
    if workspace.exists():
        workspace_content = {path.name
                             for path
                             in workspace.iterdir()}
        assert workspace_content.issubset({'metadata_generation',
                                           'validation',
                                           'preservation',
                                           'file_cache'})


@pytest.mark.usefixtures("testmongoclient", "mock_luigi_config_path")
@pytest.mark.parametrize(
    "files",
    [
        # Dataset with one text file
        [
            {
                'metadata': TXT_FILE,
                'path': 'tests/data/sample_files/text_plain_UTF-8'
            }
        ],
        # Dataset with a file in upload-rest-api
        [
            {
                'metadata': PAS_STORAGE_TXT_FILE,
                'path': 'tests/data/sample_files/text_plain_UTF-8'
            }
        ],
        # Dataset with a file named "mets.xml"
        [
            {
                'metadata': METS_FILE,
                'path': 'tests/data/sample_files/text_plain_UTF-8'
            }
        ],
        # Dataset with a file named "signature.sig"
        [
            {
                'metadata': SIG_FILE,
                'path': 'tests/data/sample_files/text_plain_UTF-8'
            }
        ],
        # Dataset with different file formats producing different
        # metadata
        [
            # text (charset metadata)
            {
                'metadata': TXT_FILE,
                'path': 'tests/data/sample_files/text_plain_UTF-8'
            },
            # CSV (ADDML)
            {
                'metadata': CSV_FILE,
                'path': 'tests/data/sample_files/text_csv.csv'
            },
            # image (MIX)
            {
                'metadata': TIFF_FILE,
                'path': 'tests/data/sample_files/valid_tiff.tiff'
            },
            # audio (AudioMD)
            {
                'metadata': AUDIO_FILE,
                'path': 'tests/data/sample_files/audio_x-wav.wav'
            },
            # video (VideoMD)
            {
                'metadata': VIDEO_FILE,
                'path': 'tests/data/sample_files/video_dv.dv'
            },
            # video container with video and audio (VideoMD and
            # AudioMD)
            {
                'metadata': MKV_FILE,
                'path': 'tests/data/sample_files/video_ffv1.mkv'
            },
            # other (no extra metadata)
            {
                'metadata': PDF_FILE,
                'path': 'tests/data/sample_files/application_pdf.pdf'
            }
        ],
        # Dataset with a file that goes to bit-level preservation
        [
            {
                'metadata': SEG_Y_FILE,
                'path': (
                    'tests/data/sample_files/invalid_1.0_ascii_header.sgy'
                )
            }
        ],
    ]
)
def test_mets_creation(config, tmp_path, workspace, requests_mock,
                       files, upload_projects_path):
    """Test SIP validity.

    Run CompressSIP task (and all tasks it requires) and check that:

        #. mets.xml validates against the schema
        #. mets.xml passes schematron verification
        #. all files are found in correct path

    :param config: Configuration file
    :param tmp_path: temporary directory
    :param workspace: temporary workspace directory
    :param requests_mock: Mocker object
    :param files: list of files to be added to dataset
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["id"] = workspace.name
    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=[file['metadata'] for file in files]
    )

    # Mock file download sources
    for file in files:
        if file['metadata']['storage_service'] == PAS_STORAGE_SERVICE:
            # Mock upload-rest-api
            file_storage_path = (upload_projects_path / "project_id"
                                 / file["metadata"]["id"])
            FileEntry(
                id=str(file_storage_path),
                checksum="2eeecd72c567401e6988624b179d0b14",
                identifier=file["metadata"]["storage_identifier"]
            ).save()
            file_storage_path.parent.mkdir()
            shutil.copy(file['path'], file_storage_path)
        else:
            # Mock Ida
            with open(file['path'], 'rb') as open_file:
                tests.utils.add_mock_ida_download(
                    requests_mock=requests_mock,
                    dataset_id=workspace.name,
                    filename=file['metadata']["pathname"],
                    content=open_file.read()
                )

    assert luigi.build(
        [siptools_research.workflow.compress.CompressSIP(
            dataset_id=workspace.name,
            config=config
        )],
        local_scheduler=True
    )

    # Extract SIP
    with tarfile.open(workspace
                      / 'preservation'
                      / f'{workspace.name}.tar') as tar:
        tar.extractall(tmp_path / 'extracted_sip')

    # Read mets.xml
    mets = lxml.etree.parse(str(tmp_path / 'extracted_sip' / 'mets.xml'))

    # Validate mets.xml against schema
    schema = lxml.etree.XMLSchema(lxml.etree.parse(METS_XSD))
    assert schema.validate(mets)

    # Validate mets.xml against Schematrons
    for schematron in _get_schematrons():
        schematron.assertValid(mets)

    # Check that all files are included in SIP
    for file in files:
        file_in_sip = (
            tmp_path / "extracted_sip" / "dataset_files"
            / file["metadata"]["pathname"].strip('/')
        )
        assert filecmp.cmp(file_in_sip, file['path'])
