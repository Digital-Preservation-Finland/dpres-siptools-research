"""Test that packaging workflow produces valid SIPs."""

import copy
import filecmp
import os
import shutil
import tarfile

import pymongo
import pytest
from siptools.xml.mets import NAMESPACES
import luigi
import lxml.etree as ET
from lxml.isoschematron import Schematron

from siptools_research.workflow.compress import CompressSIP
import siptools_research.config
import tests.metax_data.contracts
from tests.metax_data.files import PAS_STORAGE_ID
import tests.utils


METS_XSD = "/etc/xml/dpres-xml-schemas/schema_catalogs/schemas/mets/mets.xsd"
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
PAS_STORAGE_TXT_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
PAS_STORAGE_TXT_FILE["file_storage"]["identifier"] = PAS_STORAGE_ID
XML_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
XML_FILE["file_path"] = "mets.xml"
SIG_FILE = copy.deepcopy(tests.metax_data.files.TXT_FILE)
SIG_FILE["file_path"] = "signature.sig"
TIFF_FILE = copy.deepcopy(tests.metax_data.files.TIFF_FILE)
MKV_FILE = copy.deepcopy(tests.metax_data.files.MKV_FILE)
DATASET_WITH_PROVENANCE \
    = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
DATASET_WITH_PROVENANCE["research_dataset"]["provenance"] \
    = [tests.metax_data.datasets.BASE_PROVENANCE]


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
        # Dataset with multiple files
        (
            tests.metax_data.datasets.BASE_DATASET,
            [
                {
                    'metadata': tests.metax_data.files.TXT_FILE,
                    'path': 'tests/data/sample_files/text_plain_UTF-8'
                },
                {
                    'metadata': MKV_FILE,
                    'path': 'tests/data/sample_files/video_ffv1.mkv'
                },
                {
                    'metadata': TIFF_FILE,
                    'path': 'tests/data/sample_files/valid_tiff.tiff'
                }
            ]
        )
    ]
)
def test_mets_creation(testpath, pkg_root, requests_mock, dataset, files):
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
    :param pkg_root: temporary packaging root directory
    :param requests_mock: Mocker object
    :param dataset: dataset metadata
    :param files: list of file metadata objects
    :returns: ``None``
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[file['metadata'] for file in files])

    # Mock file download sources
    for file in files:
        if file['metadata']['file_storage']['identifier'] == PAS_STORAGE_ID:
            # Mock upload-rest-api
            conf = siptools_research.config.Configuration(
                tests.conftest.TEST_CONFIG_FILE
            )
            mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
            mongoclient.upload.files.insert_one(
                {
                    "identifier": file['metadata']['identifier'],
                    "_id": os.path.join(testpath,
                                        file['metadata']['identifier'])
                }
            )
            shutil.copy(file['path'],
                        testpath / file['metadata']["identifier"])
        else:
            # Mock Ida
            with open(file['path'], 'rb') as open_file:
                tests.utils.add_mock_ida_download(
                    requests_mock=requests_mock,
                    dataset_id="dataset_identifier",
                    filename=file['metadata']["file_path"],
                    content=open_file.read()
                )

    workspace = pkg_root / 'workspaces' / 'workspace'
    assert luigi.build(
        [CompressSIP(
            workspace=str(workspace),
            dataset_id='dataset_identifier',
            config=tests.conftest.UNIT_TEST_CONFIG_FILE
        )],
        local_scheduler=True
    )

    # Extract SIP
    with tarfile.open(workspace / 'workspace.tar') as tar:
        tar.extractall(testpath / 'extracted_sip')

    # Read mets.xml
    mets = ET.parse(str(testpath / 'extracted_sip' / 'mets.xml'))

    # Validate mets.xml against schema
    schema = ET.XMLSchema(ET.parse(METS_XSD))
    assert schema.validate(mets)

    # Validate mets.xml against Schematrons
    for schematron in SCHEMATRONS:
        Schematron(ET.parse(schematron)).assertValid(mets)

    # Check mets root element contract identifier and spec version
    mets_xml_root = mets.getroot()
    assert mets_xml_root.xpath('@*[local-name() = "CONTRACTID"]')[0] \
        == 'urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd'
    assert mets_xml_root.xpath('@*[local-name() = "CATALOG"] | '
                               '@*[local-name() = "SPECIFICATION"]')[0][:3] \
        == '1.7'

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
