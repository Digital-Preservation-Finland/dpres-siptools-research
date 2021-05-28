"""Test that packaging workflow produces valid SIPs."""

import copy
import os
import tarfile

import pymongo
import pytest
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


@pytest.mark.usefixtures(
    'testmongoclient', 'mock_luigi_config_path', 'mock_filetype_conf'
)
@pytest.mark.parametrize(
    ['dataset', 'files'],
    [
        (
            tests.metax_data.datasets.BASE_DATASET,
            [tests.metax_data.files.TXT_FILE]
        ),
        (
            tests.metax_data.datasets.BASE_DATASET,
            [PAS_STORAGE_TXT_FILE]
        ),
        (
            tests.metax_data.datasets.BASE_DATASET,
            [XML_FILE]
        ),
        (
            tests.metax_data.datasets.BASE_DATASET,
            [SIG_FILE]
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
                                  files=files)

    # Mock file download sources
    for file_ in files:
        if file_['file_storage']['identifier'] == PAS_STORAGE_ID:
            # Mock upload-rest-api
            conf = siptools_research.config.Configuration(
                tests.conftest.TEST_CONFIG_FILE
            )
            mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
            mongoclient.upload.files.insert_one(
                {
                    "_id": file_['identifier'],
                    "file_path": os.path.join(testpath, file_['identifier'])
                }
            )
            (testpath / file_["identifier"]).write_text("foo")
        else:
            # Mock Ida
            requests_mock.get(
                'https://ida.test/files/pid:urn:identifier/download',
                text='foo'
            )

    workspace = pkg_root / 'workspaces' / 'workspace'
    luigi.build(
        [CompressSIP(
            workspace=str(workspace),
            dataset_id='dataset_identifier',
            config=tests.conftest.UNIT_TEST_CONFIG_FILE
        )],
        local_scheduler=True
    )

    # Extract SIP
    with tarfile.open((workspace / 'workspace.tar')) as tar:
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
    for file_metadata in files:
        path = (
            testpath / "extracted_sip" / "dataset_files"
            / file_metadata["file_path"]
        )
        assert path.read_text() == "foo"
