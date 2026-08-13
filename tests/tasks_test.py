"""Unit tests for :mod:`siptools_research.tasks` package."""
import copy
import filecmp
import importlib
import json
import shutil
import tarfile

import luigi
import lxml.etree
import pytest
from lxml.isoschematron import Schematron

import siptools_research.tasks.compress
import tests.metax_data.reference_data
import tests.utils
from siptools_research.models.dataset_entry import DatasetEntry
from metax_access.template_data import DATASET
from tests.metax_data.files import (AUDIO_FILE, CSV_FILE, MKV_FILE,
                                    PAS_STORAGE_SERVICE, PDF_FILE, SEG_Y_FILE,
                                    TIFF_FILE, TXT_FILE, VIDEO_FILE)
from tests.utils import add_dps_transfer_mock, add_dps_upload_mock

DPRES_XML_SCHEMAS_CATALOG = "/etc/xml/dpres-xml-schemas/schema_catalogs/catalog_main.xml"
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
        # TODO: GetSIPStatus can not be tested like other tasks,
        # because this tets assumes that timestamp is added to database
        # once the task has finished, which currently is not true for
        # GetSIPStatus task. See TPASPKT-435 for more
        # information. Therefore, this test case is disabled.
        # ("get_sip_status", "GetSIPStatus"),
    ]
)
def test_workflow(workspace, module_name, task, requests_mock, config,
                  ida_mount_path):
    """Test workflow dependency tree.

    Each workflow task should be able complete if it is directly called
    by luigi i.e. each task should know which other tasks are required
    to complete before it itself can be run. This test runs a task (and
    all tasks it requires) and checks that report of successful task is
    added to mongodb. The output of task is NOT examined. Metax, Ida,
    mongodb and the DPS REST API are mocked.

    :param workspace: temporary workspace directory
    :param module_name: submodule of siptools_research.tasks that
                        contains Task to be tested
    :param task: Task class name
    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    :param ida_mount_path: Path to mock IDA mountpoin
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    dataset["persistent_identifier"] = "doi:test"
    tests.utils.add_metax_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)
    contract_id = dataset['preservation']['contract']

    # Mock Ida
    tests.utils.add_mock_ida_file(
        ida_mount_path=ida_mount_path,
        project_id=TXT_FILE['csc_project'],
        filename="/path/to/file",
        content=b"foo\n"
    )

    # Mock DPS
    transfer_id = f"{workspace.name}.tar-00000000-0000-0000-0000-000000000001"
    add_dps_upload_mock(requests_mock, transfer_id)
    add_dps_transfer_mock(
        requests_mock,
        contract_id=contract_id,
        transfer_id=transfer_id,
        status="accepted",
    )

    module = importlib.import_module('siptools_research.tasks.'
                                     + module_name)
    task_class = getattr(module, task)

    # The workflow is run twice because GetSIPStatus in get_sip_status.py
    # is an external task and triggers only with the second run.
    luigi.build(
        [task_class(
            dataset_id=workspace.name,
            config=config
        )],
        local_scheduler=True
    )
    luigi.build(
        [task_class(
            dataset_id=workspace.name,
            config=config
        )],
        local_scheduler=True
    )

    # Check that logs were created
    entry = DatasetEntry.objects.all()[0]
    assert entry.workflow_tasks[task].result == 'success'

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
                       monkeypatch, files, upload_projects_path, ida_mount_path):
    """Test SIP validity.

    Run CompressSIP task (and all tasks it requires) and check that:

        #. mets.xml validates against the schema
        #. mets.xml passes schematron verification
        #. all files are found in correct path

    :param config: Configuration file
    :param tmp_path: temporary directory
    :param workspace: temporary workspace directory
    :param requests_mock: Mocker object
    :param monkeypatch: Monkeypatch object
    :param files: list of files to be added to dataset
    :param upload_projects_path: Path to mock upload project
    :param ida_mount_path: Path to mock IDA mountpoint
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
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
            file_storage_path = (upload_projects_path
                                 / "test_project"
                                 / file["metadata"]["pathname"].strip("/"))

            file_storage_path.parent.mkdir(parents=True)
            shutil.copy(file['path'], file_storage_path)
        else:
            # Mock Ida
            with open(file['path'], 'rb') as open_file:
                tests.utils.add_mock_ida_file(
                    ida_mount_path=ida_mount_path,
                    project_id=file['metadata']['csc_project'],
                    filename=file['metadata']["pathname"],
                    content=open_file.read()
                )

    assert luigi.build(
        [siptools_research.tasks.compress.CompressSIP(
            dataset_id=workspace.name,
            config=config
        )],
        local_scheduler=True
    )

    # Extract SIP
    with tarfile.open(workspace
                      / 'preservation'
                      / f'{workspace.name}.tar') as tar:
        tar.extractall(
            tmp_path / 'extracted_sip',
            # Avoid warnings
            filter="data",
        )

    # Read mets.xml
    mets = lxml.etree.parse(str(tmp_path / 'extracted_sip' / 'mets.xml'))

    # Validate mets.xml against schema
    monkeypatch.setenv("XML_CATALOG_FILES", DPRES_XML_SCHEMAS_CATALOG)
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
