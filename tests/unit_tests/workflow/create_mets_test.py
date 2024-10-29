"""Tests for module :mod:`siptools_research.workflow.create_mets`."""
import copy

import lxml.etree
import pytest

import tests.utils
from siptools_research.exceptions import InvalidFileMetadataError
from siptools_research.workflow.create_mets import CreateMets
from tests.conftest import UNIT_TEST_CONFIG_FILE
from tests.metax_data.datasets import (
    BASE_DATACITE,
    BASE_DATASET,
    BASE_PROVENANCE,
    QVAIN_PROVENANCE,
)
from tests.metax_data.files import CSV_FILE, TIFF_FILE, TXT_FILE

NAMESPACES = {
    "mets": "http://www.loc.gov/METS/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "premis": "info:lc/xmlns/premis-v2",
    "fi": "http://digitalpreservation.fi/schemas/mets/fi-extensions",
    "xlink": "http://www.w3.org/1999/xlink",
    "mix": "http://www.loc.gov/mix/v20",
    "audiomd": "http://www.loc.gov/audioMD/",
    "videomd": "http://www.loc.gov/videoMD/",
    "datacite": "http://datacite.org/schema/kernel-4",
    "ead3": "http://ead3.archivists.org/schema/",
    "addml": "http://www.arkivverket.no/standarder/addml",
}


@pytest.mark.parametrize(
    'data_catalog,objid',
    [
        ('urn:nbn:fi:att:data-catalog-ida', 'doi:pas-version-id'),
        ('urn:nbn:fi:att:data-catalog-pas', 'doi:test')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_mets(workspace, requests_mock, data_catalog, objid):
    """Test the workflow task CreateMets.

    Creates METS for a dataset that contains one text file.

    Tests that:
    * the task is complete when METS has been created.
    * METS root element and METS header contain correct information.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :param data_catalog: Data catalog identifier of dataset
    :param objid: Identifier expected to be used as OBJID
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    files = [copy.deepcopy(TXT_FILE)]
    dataset['identifier'] = workspace.name
    dataset['data_catalog']['identifier'] = data_catalog
    dataset['preservation_dataset_version'] \
        = {'preferred_identifier': 'doi:pas-version-id'}
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=files
    )

    # Add text file to "dataset_files" directory
    # TODO: This file is not really used during the test, but it is
    # required because siptools-ng will raise exception if it does not
    # exist
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    task = CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created mets.xml
    mets = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))

    # Check that the root element contains expected attributes.
    mets_attributes = {
        'PROFILE': 'http://digitalpreservation.fi/mets-profiles/research-data',
        f"{{{NAMESPACES['fi']}}}CONTRACTID":
        "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
        f"{{{NAMESPACES['fi']}}}CONTENTID": objid,
    }
    assert mets_attributes.items() <= dict(mets.getroot().attrib).items()

    # The organization of the agreement should be the archivist
    metshdr = mets.xpath('/mets:mets/mets:metsHdr', namespaces=NAMESPACES)[0]
    archivist = metshdr.xpath("mets:agent[@ROLE='ARCHIVIST']",
                              namespaces=NAMESPACES)[0]
    assert archivist.attrib['TYPE'] == 'ORGANIZATION'
    assert archivist.xpath("mets:name", namespaces=NAMESPACES)[0].text \
        == "Testiorganisaatio"

    # Packaging service should be the creator of the SIP
    creator = metshdr.xpath("mets:agent[@ROLE='CREATOR']",
                            namespaces=NAMESPACES)[0]
    assert creator.attrib['TYPE'] == 'OTHER'
    assert creator.attrib['OTHERTYPE'] == 'SOFTWARE'
    assert creator.xpath("mets:name", namespaces=NAMESPACES)[0].text \
        == "Packaging Service"


def test_idempotence(workspace, requests_mock):
    """Test that CreateMets task is idempotent.

    Run the task twice and ensure that the created METS document is
    identical, excluding random identifiers and dates that are generated
    during the process.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    files = [copy.deepcopy(TXT_FILE)]
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=files
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    task = CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Stash the created METS document to workspace root
    (workspace / 'preservation' / 'mets.xml').rename(workspace / 'mets1.xml')
    assert not task.complete()

    # Rerun the task
    task.run()
    assert task.complete()

    # Compare the created METS documents.
    # The documents should be mostly the same, but some lines contain
    # random identifiers, creation dates etc., and the elements are not
    # always in the same order, so it is hard to reliably test if they
    # are similar. But at least the documents should have the same size.
    mets1 = workspace / 'mets1.xml'
    mets2 = workspace / 'preservation' / 'mets.xml'
    assert mets1.stat().st_size == mets2.stat().st_size


METADATA_MODIFICATION_PROVENANCE = copy.deepcopy(BASE_PROVENANCE)
METADATA_MODIFICATION_PROVENANCE["preservation_event"]["pref_label"]["en"]\
    = "metadata modification"
ANOTHER_BASE_PROVENANCE = copy.deepcopy(BASE_PROVENANCE)
ANOTHER_BASE_PROVENANCE["description"]["en"] = "another description"


@pytest.mark.usefixtures("testmongoclient")
@pytest.mark.parametrize(
    "provenance_data",
    [
        # 0 events
        [],
        # 1 event
        [BASE_PROVENANCE],
        # multiple events
        [BASE_PROVENANCE, METADATA_MODIFICATION_PROVENANCE],
        # Two provenence events with same event type
        [BASE_PROVENANCE, ANOTHER_BASE_PROVENANCE],
        # provenance event made in Qvain
        [QVAIN_PROVENANCE],
    ]
)
def test_multiple_provenance_events(workspace,
                                    requests_mock,
                                    provenance_data):
    """Test creating PREMIS metadata for multiple provenance events.

    Creates METS document dataset that has multiple provenance events.

    Tests that:
    * PREMIS event metadata is created for each provenance event
    * The PREMIS event metadata is referenced in physical structure map
    * The PREMIS event metadata is referenced in logical structure map

    :param workspace: Testpath fixture
    :param requests_mock: HTTP request mocker
    :param provenance_data: List of provenance events in dataset
    """
    # Mock metax. Create a dataset with provenance events.
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    # Add use category to a file to ensure that logical structuremap is
    # created

    # V3 does not support saving the files use category from research dataset
    dataset["research_dataset"]["files"] = [
        {
            "identifier": "pid:urn:identifier",
            "use_category": {
                "pref_label": {
                    "en": "dummy-use-category"
                }
            }
        }
    ]
    if provenance_data:
        dataset['research_dataset']['provenance'] = provenance_data
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[TXT_FILE]
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Find identifiers of provenance events from METS
    mets = lxml.etree.parse(str(workspace / "preservation" / "mets.xml"))
    provenance_ids = []
    for provenance in provenance_data:
        if "preservation_event" in provenance:
            event_type = provenance["preservation_event"]["pref_label"]["en"]
            event_detail = provenance["description"]["en"]
        else:
            event_type = provenance["lifecycle_event"]["pref_label"]["en"]
            event_detail = provenance["title"]["en"]
        digiprovmd = mets.xpath(
            f"//*[premis:eventType='{event_type}'"
            f" and premis:eventDetail='{event_detail}']"
            "/ancestor::mets:digiprovMD",
            namespaces=NAMESPACES)
        assert len(digiprovmd) == 1
        provenance_ids.append(digiprovmd[0].attrib["ID"])

    # PREMIS events should be refenced in physical structure map
    physical_structmap = mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="PHYSICAL"]',
        namespaces=NAMESPACES
    )[0]
    physical_structmap_references \
        = physical_structmap.xpath('mets:div/@ADMID', namespaces=NAMESPACES)[0]
    for provenance_id in provenance_ids:
        assert provenance_id in physical_structmap_references

    # PREMIS events should be refenced in logical structure map
    logical_structmap = mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="Fairdata-logical"]',
        namespaces=NAMESPACES
    )[0]
    logical_structmap_reference_list = logical_structmap.xpath(
        'mets:div/@ADMID',
        namespaces=NAMESPACES
    )
    if logical_structmap_reference_list:
        logical_structmap_references = logical_structmap_reference_list[0]
    else:
        # Logical structure map does not have any ADMIDs if user does
        # not provide any provenance events
        logical_structmap_references = []
    for provenance_id in provenance_ids:
        assert provenance_id in logical_structmap_references


@pytest.mark.parametrize(
    'provenance_data', [BASE_PROVENANCE, QVAIN_PROVENANCE]
)
def test_premis_event_metadata(
    workspace, requests_mock, provenance_data
):
    """Test creating PREMIS events for provenance metadata.

    Creates METS for dataset that has provenance event.

    Tests that PREMIS event metadata created for provenance event
    contains correct information.

    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param provenance_data: The data used for creating provenance events
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    dataset["research_dataset"]["provenance"] = [provenance_data]
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset,
        files=[TXT_FILE]
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Find the digiprovMD element of provenance event from METS document
    if 'description' in provenance_data:
        expected_event_detail = provenance_data['description']['en']
    else:
        expected_event_detail = provenance_data['title']['en']
    mets = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))
    digiprovmd = mets.xpath(
        f"//*[premis:eventDetail='{expected_event_detail}']"
        "/ancestor::mets:digiprovMD",
        namespaces=NAMESPACES
    )[0]

    # Check that created PREMIS event contains correct information
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventType',
        namespaces=NAMESPACES
    )
    assert elements[0].text == "creation"

    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventDateTime',
        namespaces=NAMESPACES
    )

    if "temporal" in provenance_data:
        assert elements[0].text == "2014-01-01T08:19:58Z"
    else:
        assert elements[0].text == "OPEN"

    # Title and description should be formatted together as "title:
    # description" or just as is if the other one does not exist
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventDetail',
        namespaces=NAMESPACES
    )
    if "title" in provenance_data and "description" in provenance_data:
        assert elements[0].text == "Title: Description of provenance"
    elif "title" in provenance_data:
        assert elements[0].text == "Title"
    elif "description" in provenance_data:
        assert elements[0].text == "Description of provenance"
    else:
        # Invalid provenance, there is no title or description
        assert False

    # Outcome should be "(:unav)" if missing
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcome',
        namespaces=NAMESPACES)
    if "event_outcome" in provenance_data:
        assert elements[0].text == "success"
    else:
        assert elements[0].text == "(:unav)"

    # Outcome description is optional
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcomeDetail'
        '/premis:eventOutcomeDetailNote',
        namespaces=NAMESPACES
    )
    if "outcome_description" in provenance_data:
        assert elements[0].text == "outcome_description"
    else:
        assert elements == []


@pytest.mark.parametrize(
    "event_outcome_identifier,expected_event_outcome",
    [
        ("http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success",
         "success"),
        ("http://uri.suomi.fi/codelist/fairdata/event_outcome/code/failure",
         "failure"),
        ("http://uri.suomi.fi/codelist/fairdata/event_outcome/code/unknown",
         "(:unkn)"),
        # In Metax the URI is all lowercase, but in uri.suomi.fi it is
        # not. Both are supported just in case.
        ("http://uri.suomi.fi/codelist/fairdata/event_outcome/code/Success",
         "success"),
    ]
)
def test_premis_event_outcome(workspace, requests_mock,
                              event_outcome_identifier,
                              expected_event_outcome):
    """Test that correct PREMIS eventOutcome is used.

    Creates METS for dataset that has provenance event with
    identified by `event_outcome_identifier`.

    Checks that expected eventOutcome value is written to PREMIS event
    metadata.

    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param event_outcome_identifier: event outcome identifier in Metax
                                     metadata
    :param expected_event_outcome: event outcome value that should be
                                   written to METS
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    provenance = copy.deepcopy(BASE_PROVENANCE)
    provenance["event_outcome"]["identifier"] = event_outcome_identifier
    dataset["research_dataset"]["provenance"] = [provenance]
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset,
        files=[TXT_FILE]
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Check that correct event outcome is written to METS
    mets = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))
    digiprovmd = mets.xpath(
        "//*[premis:eventDetail='Description of provenance']"
        "/ancestor::mets:digiprovMD",
        namespaces=NAMESPACES
    )[0]
    outcome = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcome',
        namespaces=NAMESPACES)
    assert outcome[0].text == expected_event_outcome


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata(workspace, requests_mock):
    """Test descriptive metadata creation.

    Creates METS for a simple dataset.

    Tests that:
    * datacite XML is imported to dmdSec of METS
    * dmdSec is referenced in both structure maps

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    # Add use category to a file to ensure that logical structuremap is
    # created
    dataset["research_dataset"]["files"] = [
        {
            "identifier": "pid:urn:identifier",
            "use_category": {
                "pref_label": {
                    "en": "dummy-use-category"
                }
            }
        }
    ]
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset,
        files=[TXT_FILE]
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Check that METS document contains correct elements.
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))

    # The mdWrap element should contain the datacite metadata
    dmdsec = mets.xpath('/mets:mets//mets:dmdSec', namespaces=NAMESPACES)[0]
    mdwrap = dmdsec.xpath('mets:mdWrap', namespaces=NAMESPACES)
    assert mdwrap[0].attrib["OTHERMDTYPE"] == "DATACITE"
    assert mdwrap[0].attrib["MDTYPEVERSION"] == "4.1"
    mets_datacite = mdwrap[0].xpath('mets:xmlData/datacite:resource',
                                    namespaces=NAMESPACES)[0]

    # Compare datacite metadata in METS file to the original datacite
    # metadata retrieved from metax. The canonical string presentations
    # should be identical.
    mets_datacite = lxml.etree.fromstring(lxml.etree.tostring(mets_datacite))
    metax_datacite = BASE_DATACITE.getroot()
    assert lxml.etree.tostring(mets_datacite,
                               strip_text=True,
                               method="c14n2") \
        == lxml.etree.tostring(metax_datacite,
                               strip_text=True,
                               method="c14n2")

    # Check that descriptive metadata is referenced in PHYSICAL
    # structmap
    structmap = mets.xpath("//mets:structMap[@TYPE='PHYSICAL']",
                                    namespaces=NAMESPACES)[0]
    structmap_div = structmap.xpath("mets:div", namespaces=NAMESPACES)[0]
    assert structmap_div.attrib["DMDID"] == dmdsec.attrib["ID"]

    # Check that descriptive metadata is referenced in Fairdata-logical
    # structmap
    structmap = mets.xpath("//mets:structMap[@TYPE='Fairdata-logical']",
                                    namespaces=NAMESPACES)[0]
    structmap_div = structmap.xpath("mets:div", namespaces=NAMESPACES)[0]
    assert structmap_div.attrib["DMDID"] == dmdsec.attrib["ID"]


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd(workspace, requests_mock):
    """Test technical metadata creation.

    Creates METS for a dataset that contains one TIFF file.

    Tests that file metadata (file format, format version, checksum,
    checksum algorithm and creation date) from Metax is used to create
    PREMIS object.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[TIFF_FILE]
    )

    # Create workspace that already contains the dataset files
    dataset_files_parent = workspace / 'metadata_generation'
    tiff_path = 'dataset_files/' + TIFF_FILE['file_path'].strip('/')
    (dataset_files_parent / tiff_path).parent.mkdir(parents=True)
    (dataset_files_parent / tiff_path).touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Read created METS
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # Check that the PREMIS object element has desired properties
    premis_object_element \
        = mets.xpath("//premis:object", namespaces=NAMESPACES)[0]
    assert premis_object_element.xpath(
        "//premis:formatName", namespaces=NAMESPACES
    )[0].text == TIFF_FILE["file_characteristics"]["file_format"]
    assert premis_object_element.xpath(
        "//premis:formatVersion",
        namespaces=NAMESPACES
    )[0].text == TIFF_FILE["file_characteristics"]["format_version"]
    assert premis_object_element.xpath(
        "//premis:messageDigestAlgorithm",
        namespaces=NAMESPACES
    )[0].text == TIFF_FILE["checksum"]["algorithm"]
    assert premis_object_element.xpath(
        "//premis:messageDigest",
        namespaces=NAMESPACES
    )[0].text == f'{TIFF_FILE["checksum"]["value"]}'
    assert premis_object_element.xpath(
        "//premis:dateCreatedByApplication",
        namespaces=NAMESPACES
    )[0].text == TIFF_FILE["file_characteristics"]["file_created"]



@pytest.mark.parametrize(
    ("checksum_algorithm", "encoding"),
    [
        # These probably are the defaults
        ("MD5", "UTF-8"),
        # and these are not
        ("SHA-256", "ISO-8859-15")
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_user_defined_techmd(workspace, requests_mock, checksum_algorithm,
                             encoding):
    """Test using user defined values.

    Creates METS for a dataset that contains one text file with
    predefined checksum algorithm and encoding.

    Tests that the user defined values from Metax metadata are used
    instead of the default values of siptools-ng.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param checksum_algorithm: Checksum algorithm to file medatada
    :param encoding: File encoding to file medatada
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    file = copy.deepcopy(TXT_FILE)
    file["file_characteristics"]["encoding"] = encoding
    file["file_characteristics_extension"]["streams"][0]["charset"] \
        = encoding
    file["checksum"]["algorithm"] = checksum_algorithm
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        dataset=dataset,
        files=[file]
    )

    # Create workspace that already contains the dataset files
    filepath = workspace / "metadata_generation/dataset_files" \
        / file["file_path"].strip('/')
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Read created METS
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # Check that the PREMIS object element has desired properties
    premis_object_element \
        = mets.xpath("//premis:object", namespaces=NAMESPACES)[0]
    assert premis_object_element.xpath(
        "//premis:formatName", namespaces=NAMESPACES
    )[0].text == f"text/plain; charset={encoding}"
    assert premis_object_element.xpath(
        "//premis:messageDigestAlgorithm",
        namespaces=NAMESPACES
    )[0].text == checksum_algorithm


@pytest.mark.parametrize(
    'has_header,expected_field_definition',
    [
        # If csv does not have header, default field definition
        # 'header1' will be used
        (False, "header1"),
        # If csv has header, the field definition will be read from the
        # file
        (True, 'a'),
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_csv(workspace, requests_mock, has_header,
                           expected_field_definition):
    """Test that technical metadata is created correctly for csv files.

    Create METS for dataset that contains text file that user has
    defined as csv file.

    Tests that:
    * file metadata (file format and encoding) from Metax is used to
      create PREMIS object
    * CSV specific metadata from Metax is used to create ADDML metadata

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param has_header: Does the csv has header?
    :param expected_field_definition: Expected definition of the first
                                      field of CSV file
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    file = copy.deepcopy(CSV_FILE)
    file['file_characteristics']['csv_has_header'] = has_header
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=[file]
    )

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file.csv"
    filepath.parent.mkdir(parents=True)
    filepath.touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Read created METS
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # The file format should be text/csv
    premis_object_element \
        = mets.xpath("//premis:object", namespaces=NAMESPACES)[0]
    assert premis_object_element.xpath(
        "//premis:formatName", namespaces=NAMESPACES
    )[0].text == "text/csv; charset=UTF-8"

    # Check that addml metadata contains expected information
    assert mets.xpath("//addml:recordSeparator",
                      namespaces=NAMESPACES)[0].text \
        == file["file_characteristics"]["csv_record_separator"]
    assert mets.xpath("//addml:fieldSeparatingChar",
                      namespaces=NAMESPACES)[0].text \
        == file["file_characteristics"]["csv_delimiter"]
    assert mets.xpath("//addml:quotingChar",
                      namespaces=NAMESPACES)[0].text \
        == file["file_characteristics"]["csv_quoting_char"]
    assert mets.xpath("//addml:fieldDefinition/@name",
                      namespaces=NAMESPACES)[0] == expected_field_definition


@pytest.mark.usefixtures('testmongoclient')
def test_create_filesec_and_structmap(workspace, requests_mock):
    """Test fileSec and physical structure map creation.

    Creates METS for a dataset that contains three files in a directory
    structure.

    Tests that
    * The files are added to fileSec
    * PYSICAL structMap contains expected information

    :param workspace: Temporary workspace fixture
    :param requests_mock: HTTP request mocker
    """
    # Create dataset that contains three text files
    files = []
    files = [copy.deepcopy(TXT_FILE) for i in range(3)]
    files[0]["file_path"] = "/file1"
    files[1]["file_path"] = "/file2"
    files[2]["file_path"] = "/subdirectory/file3"
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["identifier"] = workspace.name
    # Add provenance events to dataset
    tests.utils.add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=dataset,
        files=files
    )

    # Create files in workspace
    dataset_files = workspace / "metadata_generation/dataset_files"
    subdirectory = dataset_files / "subdirectory"
    subdirectory.mkdir(parents=True)
    (dataset_files / "file1").touch()
    (dataset_files / "file2").touch()
    (subdirectory / "file3").touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Read created METS document
    # NOTE: lxml<4.8 requires path as string. Newer versions support
    # Path objects!
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))

    # fileSec should contain three files
    files = mets.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file/mets:FLocat/'
        '@xlink:href',
        namespaces=NAMESPACES
    )
    assert len(files) == 3
    assert set(files) == {'file:///dataset_files/file1',
                          'file:///dataset_files/file2',
                          'file:///dataset_files/subdirectory/file3'}

    # Check that "PHYSICAL" structMap contains dataset_files directory
    # and the files.
    structmap = mets.xpath("//mets:structMap[@TYPE='PHYSICAL']",
                           namespaces=NAMESPACES)[0]
    assert structmap.xpath(
        "mets:div/mets:div/@LABEL",
        namespaces=NAMESPACES
    )[0] == 'dataset_files'
    assert len(structmap.xpath('*//mets:fptr', namespaces=NAMESPACES)) == 3


@pytest.mark.usefixtures('testmongoclient')
def test_create_logical_structmap(workspace, requests_mock):
    """Test creating logical structure map.

    Creates METS for a dataset that contains two files and tests that
    logical structure map contains correct information.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Create a dataset that contains three files
    files = [copy.deepcopy(TXT_FILE),
             copy.deepcopy(TXT_FILE),
             copy.deepcopy(TXT_FILE)]
    files[0]['identifier'] = 'fileid1'
    files[0]['file_path'] = 'files/file1'
    files[1]['identifier'] = 'fileid2'
    files[1]['file_path'] = 'files/file2'
    files[2]['identifier'] = 'fileid3'
    files[2]['file_path'] = 'files/file3'
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    # Add use category to two of the files
    dataset["research_dataset"]["files"] = [
        {
            "identifier": files[0]['identifier'],
            "use_category": {
                "pref_label": {
                    "en": "dummy-use-category"
                }
            }
        },
        {
            "identifier": files[1]['identifier'],
            "use_category": {
                "pref_label": {
                    "en": "dummy-use-category"
                }
            }
        }
    ]

    # Add dataset to Metax mock
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=files
    )

    # Create workspace that already contains dataset files
    file_directory = workspace / 'metadata_generation/dataset_files/files'
    file_directory.mkdir(parents=True)
    (file_directory / "file1").touch()
    (file_directory / "file2").touch()
    (file_directory / "file3").touch()

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Validate logical Fairdata-logical structure map
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    structmap = mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="Fairdata-logical"]',
        namespaces=NAMESPACES
    )[0]
    assert structmap.xpath('mets:div',
                           namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "logical"

    # There should be one div in structMap
    directories = structmap.xpath(
        'mets:div/mets:div',
        namespaces=NAMESPACES
    )
    assert len(directories) == 1
    assert directories[0].attrib['TYPE'] == 'dummy-use-category'

    # The div should contain two files
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:fptr',
        namespaces=NAMESPACES
    )) == 2


@pytest.mark.usefixtures('testmongoclient')
def test_empty_logical_structuremap(workspace, requests_mock):
    """Test that empty logical structuremap is not created.

    Creates METS for a dataset that contains files, but use category is
    not defined any of them. Logical structuremap should not be created.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Create a dataset that contains three files
    file = copy.deepcopy(TXT_FILE)
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name

    # Add dataset to Metax mock
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=[file]
    )

    # Create workspace that already contains dataset files
    file_path = workspace / "metadata_generation/dataset_files/" \
        / file["file_path"].strip("/")
    file_path.parent.mkdir(parents=True)
    file_path.write_text("foo")

    # Init and run task
    CreateMets(dataset_id=workspace.name, config=UNIT_TEST_CONFIG_FILE).run()

    # Validate logical Fairdata-logical structure map
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    assert not mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="Fairdata-logical"]',
        namespaces=NAMESPACES
    )


@pytest.mark.parametrize(
    "key",
    [
        "file_format",
        "encoding",
        "format_version",
        "csv_delimiter",
        "csv_record_separator",
        "csv_quoting_char",
    ]
)
# TODO: This test is probably unnecessary when issue TPASPKT-1105 has
# been resolved
def test_file_characteristics_conflict(workspace, requests_mock, key):
    """Test creating METS with conflicting file metadata.

    Create a conflict between file_characteristics and
    file_characteristics_extension by changing value of `key` in
    file_characteristics. Exception should be raised while creating
    METS.
    """
    file = copy.deepcopy(CSV_FILE)
    # Create a conflict between file_characteristics and
    # file_characteristics_extension
    file["file_characteristics"][key] = "foo"
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_v2_dataset(
        requests_mock, dataset=dataset, files=[file]
    )

    # Init and run task
    with pytest.raises(InvalidFileMetadataError,
                       match="File characteristics have changed"):
        CreateMets(dataset_id=workspace.name,
                   config=UNIT_TEST_CONFIG_FILE).run()


