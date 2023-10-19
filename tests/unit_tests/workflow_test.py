"""Unit tests for :mod:`siptools_research.workflow` package."""
from unittest import mock
import copy
import datetime
import filecmp
import importlib
import shutil
import tarfile

from lxml.isoschematron import Schematron
from siptools.xml.mets import NAMESPACES
from upload_rest_api.models.file_entry import FileEntry
import luigi
import lxml.etree as ET
import pymongo
import pytest

from siptools_research.config import Configuration
from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.compress import CompressSIP
from tests.metax_data.files import PAS_STORAGE_ID
import tests.metax_data.contracts
import tests.utils


METS_XSD = "/etc/xml/dpres-xml-schemas/schema_catalogs/schemas/mets/mets.xsd"
PAS_STORAGE_TXT_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
PAS_STORAGE_TXT_FILE["file_storage"]["identifier"] = PAS_STORAGE_ID
XML_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
XML_FILE["file_path"] = "mets.xml"
SIG_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
SIG_FILE["file_path"] = "signature.sig"
TIFF_FILE = copy.deepcopy(tests.metax_data.files.TIFF_FILE)
MKV_FILE = copy.deepcopy(tests.metax_data.files.MKV_FILE)
SEG_Y_FILE = copy.deepcopy(tests.metax_data.files.SEG_Y_FILE)
DATASET_WITH_PROVENANCE \
    = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
DATASET_WITH_PROVENANCE["research_dataset"]["provenance"] \
    = [tests.metax_data.datasets.BASE_PROVENANCE]

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


def _get_schematrons():
    """Get cached Schematrons.

    Parsing Schematron files is slow, so they are parsed only when this
    function is called first time.
    """
    if not SCHEMATRONS:
        for schematron_file in SCHEMATRON_FILES:
            schematron = Schematron(ET.parse(schematron_file))
            SCHEMATRONS.append(schematron)

    return SCHEMATRONS


def _mock_exists(_, path):
    return f'accepted/{datetime.date.today().strftime("%Y-%m-%d")}/' in path


@pytest.mark.parametrize(
    "module_name,task", [
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
    ]
)
@pytest.mark.usefixtures(
    'testmongoclient', 'mock_luigi_config_path', 'mock_filetype_conf'
)
def test_workflow(workspace, module_name, task, requests_mock, mocker):
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
    :param mocker: Pytest-mock mocker
    :returns: ``None``
    """
    mocker.patch('siptools_research.workflow.send_sip.paramiko.SSHClient',
                 new=mock.MagicMock)

    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name

    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[tests.metax_data.files.TXT_FILE])
    tests.utils.add_mock_ida_download(
        requests_mock=requests_mock,
        dataset_id=workspace.name,
        filename="path/to/file",
        content=b"foo"
    )

    # Init pymongo client
    conf = Configuration(tests.conftest.TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))

    with mock.patch.object(RemoteAnyTarget, '_exists', _mock_exists):
        module = importlib.import_module('siptools_research.workflow.'
                                         + module_name)
        task_class = getattr(module, task)
        luigi.build(
            [task_class(
                dataset_id=workspace.name,
                config=tests.conftest.UNIT_TEST_CONFIG_FILE
            )],
            local_scheduler=True
        )

    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()

    # Check 'result' field
    assert document['workflow_tasks'][task]['result'] == 'success'

    # The workspace root directory should contain only metadata
    # generation, validation and preservation workspaces
    if workspace.exists():
        workspace_content = {path.name
                             for path
                             in workspace.iterdir()}
        assert workspace_content.issubset({'metadata_generation',
                                           'validation',
                                           'preservation'})


@pytest.mark.usefixtures(
    'testmongoclient', 'mock_luigi_config_path', 'mock_filetype_conf'
)
@pytest.mark.parametrize(
    ['dataset', 'files'],
    [
        # Dataset with one text file
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': tests.metax_data.files.TXT_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                }
            ]
        ),
        # Dataset with a file in upload-rest-api
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': PAS_STORAGE_TXT_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                }
            ]
        ),
        # Dataset with a file named "mets.xml"
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': XML_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                }
            ]
        ),
        # Dataset with a file named "signature.sig"
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': SIG_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                }
            ]
        ),
        # Dataset with provenance information that user created
        (
            DATASET_WITH_PROVENANCE,
            [
                {
                    'metadata': tests.metax_data.files.TXT_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                }
            ]
        ),
        # Dataset with different file formats producing different
        # metadata
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                # text (charset metadata)
                {
                    'metadata': tests.metax_data.files.TXT_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                },
                # CSV (ADDML)
                {
                    'metadata': tests.metax_data.files.CSV_FILE,
                    'path': 'tests/data/sample_files/text_csv.csv'
                },
                # image (MIX)
                {
                    'metadata': TIFF_FILE,
                    'path': 'tests/data/sample_files/valid_tiff.tiff'
                },
                # audio (AudioMD)
                {
                    'metadata': tests.metax_data.files.AUDIO_FILE,
                    'path': 'tests/data/sample_files/audio_x-wav.wav'
                },
                # video (VideoMD)
                {
                    'metadata': tests.metax_data.files.VIDEO_FILE,
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
                    'metadata': tests.metax_data.files.PDF_FILE,
                    'path': 'tests/data/sample_files/application_pdf.pdf'
                }
            ]
        ),
        # Dataset with a file that goes to bit-level preservation
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': SEG_Y_FILE,
                    'path': (
                        'tests/data/sample_files/invalid_1.0_ascii_header.sgy'
                    )
                }
            ]
        )
    ]
)
def test_mets_creation(testpath, workspace, requests_mock, dataset, files,
                       upload_projects_path):
    """Test SIP validity.

    Run CompressSIP task (and all tasks it requires) and check that:

        #. mets.xml validates against the schema
        #. mets.xml passes schematron verification
        #. digital object fixity (checksums) is correct in mets.xml
        #. digital objects of the SIP are valid
        #. mets.xml root element is valid (CONTRACTID, SPECIFICATION)
        #. all files are found in correct path
        #. all provenance events are found in mets.xml

    :param testpath: temporary directory
    :param workspace: temporary workspace directory
    :param requests_mock: Mocker object
    :param dataset: dataset metadata
    :param files: list of file metadata objects
    :returns: ``None``
    """
    # Mock Metax
    dataset = copy.deepcopy(dataset)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[file['metadata'] for file in files])

    # Mock file download sources
    for file in files:
        if file['metadata']['file_storage']['identifier'] == PAS_STORAGE_ID:
            # Mock upload-rest-api
            file_storage_path = (upload_projects_path / "project_id"
                                 / file["metadata"]["identifier"])
            FileEntry(
                id=str(file_storage_path),
                checksum="2eeecd72c567401e6988624b179d0b14",
                identifier=file["metadata"]["identifier"]
            ).save()
            file_storage_path.parent.mkdir()
            shutil.copy(file['path'], file_storage_path)
        else:
            # Mock Ida
            with open(file['path'], 'rb') as open_file:
                tests.utils.add_mock_ida_download(
                    requests_mock=requests_mock,
                    dataset_id=workspace.name,
                    filename=file['metadata']["file_path"],
                    content=open_file.read()
                )

    assert luigi.build(
        [CompressSIP(
            dataset_id=workspace.name,
            config=tests.conftest.UNIT_TEST_CONFIG_FILE
        )],
        local_scheduler=True
    )

    # Extract SIP
    with tarfile.open(workspace
                      / 'preservation'
                      / f'{workspace.name}.tar') as tar:
        tar.extractall(testpath / 'extracted_sip')

    # Read mets.xml
    mets = ET.parse(str(testpath / 'extracted_sip' / 'mets.xml'))

    # Validate mets.xml against schema
    schema = ET.XMLSchema(ET.parse(METS_XSD))
    assert schema.validate(mets)

    # Validate mets.xml against Schematrons
    for schematron in _get_schematrons():
        schematron.assertValid(mets)

    # Check mets root element contract identifier and spec version
    mets_xml_root = mets.getroot()
    assert mets_xml_root.xpath('@*[local-name() = "CONTRACTID"]')[0] \
        == 'urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd'
    assert mets_xml_root.xpath('@*[local-name() = "CATALOG"] | '
                               '@*[local-name() = "SPECIFICATION"]')[0][:3] \
        == '1.7'

    # Check that files going to bit-level preservation have the correct USE
    # attribute in METS fileSec
    if files[0]['metadata'] == SEG_Y_FILE:
        assert mets_xml_root.xpath(
            '//mets:file/@USE',
            namespaces=NAMESPACES
        )[0] == 'fi-dpres-file-format-identification'

    # Check that all files are included in SIP
    for file in files:
        file_in_sip = (
            testpath / "extracted_sip" / "dataset_files"
            / file["metadata"]["file_path"]
        )
        assert filecmp.cmp(file_in_sip, file['path'])

    # Check that premis event is created for each provenance event of
    # dataset
    event_descriptions = [event['description']['en']
                          for event
                          in dataset["research_dataset"].get('provenance', [])]
    premis_event_details = [event_detail.text
                            for event_detail
                            in mets_xml_root.xpath('//premis:eventDetail',
                                                   namespaces=NAMESPACES)]
    for event_description in event_descriptions:
        assert event_description in premis_event_details
