"""Tests for module :mod:`siptools_research.workflow.create_mets`."""
import copy
import hashlib
import shutil

import lxml
import lxml.etree
from lxml import etree
import pytest
import xmltodict
from metax_access import Metax

from siptools_research.workflow.create_mets import (CreateMets,
                                                    algorithm_name,
                                                    find_dir_use_category,
                                                    get_dirpath_dict)
from siptools_research.exceptions import InvalidDatasetMetadataError
import tests.utils
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TXT_FILE, TIFF_FILE, MKV_FILE


NAMESPACES = {
    "mets": "http://www.loc.gov/METS/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "premis": "info:lc/xmlns/premis-v2",
    "fi": "http://digitalpreservation.fi/schemas/mets/fi-extensions",
    "xlink": "http://www.w3.org/1999/xlink",
    "mix": "http://www.loc.gov/mix/v20",
    "audiomd": "http://www.loc.gov/audioMD/",
    "videomd": "http://www.loc.gov/videoMD/",
    "datacite": "http://datacite.org/schema/kernel-4"
}


@pytest.mark.parametrize(
    'data_catalog,objid',
    [
        ('urn:nbn:fi:att:data-catalog-ida', 'doi:pas-version-id'),
        ('urn:nbn:fi:att:data-catalog-pas', 'doi:test')
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_mets_ok(workspace, requests_mock, data_catalog, objid):
    """Test the workflow task CreateMets.

    Test creating METS for a simple dataset that contains one text file.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :param data_catalog: Data catalog identifier of dataset
    :param objid: Identifier expected to be used as OBJID
    :returns: ``None``
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    files = [copy.deepcopy(TXT_FILE)]
    dataset['identifier'] = workspace.name
    dataset['data_catalog']['identifier'] = data_catalog
    dataset['preservation_dataset_version'] \
        = {'preferred_identifier': 'doi:pas-version-id'}
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created mets.xml
    tree = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))

    # Check that the root element contains expected attributes.
    mets_attributes = {
        'PROFILE': 'http://digitalpreservation.fi/mets-profiles/research-data',
        f"{{{NAMESPACES['fi']}}}CONTRACTID":
        "urn:uuid:abcd1234-abcd-1234-5678-abcd1234abcd",
        f"{{{NAMESPACES['xsi']}}}schemaLocation":
        'http://www.loc.gov/METS/ http://digitalpreservation.fi/'
        'schemas/mets/mets.xsd',
        f"{{{NAMESPACES['fi']}}}SPECIFICATION": '1.7.6',
        'OBJID': objid,
        f"{{{NAMESPACES['fi']}}}CONTENTID": objid,
        f"{{{NAMESPACES['fi']}}}CATALOG": '1.7.6',
    }
    assert tree.getroot().attrib == mets_attributes

    # Check that XML documents contains expected namespaces
    assert tree.getroot().nsmap == {
        key: NAMESPACES[key]
        for key in ("xsi", "mets", "fi", "premis", "xlink")
    }

    # Check metsHdr element attributes
    metshdr = tree.xpath('/mets:mets/mets:metsHdr', namespaces=NAMESPACES)[0]
    assert metshdr.attrib['RECORDSTATUS'] == 'submission'
    assert metshdr.attrib['CREATEDATE']

    # Check agent element attributes
    archivist = metshdr.xpath("mets:agent[@ROLE='ARCHIVIST']",
                              namespaces=NAMESPACES)[0]
    assert archivist.attrib['TYPE'] == 'ORGANIZATION'
    assert archivist.xpath("mets:name", namespaces=NAMESPACES)[0].text \
        == "Testiorganisaatio"
    creator = metshdr.xpath("mets:agent[@ROLE='CREATOR']",
                            namespaces=NAMESPACES)[0]
    assert creator.attrib['ROLE'] == 'CREATOR'
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
    :returns: ``None``
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    files = [copy.deepcopy(TXT_FILE)]
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Add text file to "dataset_files" directory
    filepath = workspace / "metadata_generation/dataset_files/path/to/file"
    filepath.parent.mkdir(parents=True)
    filepath.write_text('foo')

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
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


@pytest.mark.usefixtures("testmongoclient")
@pytest.mark.parametrize(
    ["events", "expected_ids", "provenance_data"],
    [
        # 0 events
        (
            [], [], None
        ),
        # 1 event
        (
            ["creation"],
            ['6fc8a863bb6ed3cee2b1e853aa38d2db'],
            tests.metax_data.datasets.BASE_PROVENANCE
        ),
        # multiple events
        (
            ["creation", "metadata modification"],
            ['6fc8a863bb6ed3cee2b1e853aa38d2db',
             'f1ffc55803b971ab8dd013710766f47e'],
            tests.metax_data.datasets.BASE_PROVENANCE
        ),
        # provenance event made in Qvain
        (
            ['creation'],
            ['56e8efc54b66b68319a3db79e6038eb3'],
            tests.metax_data.datasets.QVAIN_PROVENANCE
        )
    ]
)
def test_createprovenanceinformation(workspace,
                                     requests_mock,
                                     events,
                                     expected_ids,
                                     provenance_data):
    """Test creating METS document with multiple provenance events.

    Premis event should be created for each event.

    :param workspace: Testpath fixture
    :param requests_mock: HTTP request mocker
    :param events: list of preservation events in dataset metadata
    :param expected_ids: expected identifiers of PREMIS events that are
                         created
    :param provenance_data: The data used for creating provenance
                            events.
    """
    # Mock metax. Create a dataset with provenance events.
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    dataset['research_dataset']['provenance'] = []
    for event in events:
        provenance = copy.deepcopy(provenance_data)
        if "preservation_event" in provenance:
            provenance["preservation_event"]["pref_label"]["en"] = event
        else:
            provenance["lifecycle_event"]["pref_label"]["en"] = event
        dataset['research_dataset']['provenance'].append(provenance)
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init and run task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()
    assert task.complete()

    # PREMIS event should be created for each event
    mets = lxml.etree.parse(str(workspace / "preservation" / "mets.xml"))
    digiprovmd_elements = mets.xpath('/mets:mets/mets:amdSec/mets:digiprovMD',
                                     namespaces=NAMESPACES)
    digiprovmd_identifiers \
        = [elem.attrib['ID'].strip('_') for elem in digiprovmd_elements]
    for expected_id in expected_ids:
        assert expected_id in digiprovmd_identifiers

    # PREMIS events should be refenced in structure map
    structmap_root_div_elements \
        = mets.xpath('/mets:mets/mets:structMap/mets:div',
                     namespaces=NAMESPACES)
    structmap_references = [
        identifier.strip("_")
        for identifier
        in structmap_root_div_elements[0].attrib.get('ADMID', "").split()
    ]
    for expected_id in expected_ids:
        assert expected_id in structmap_references


@pytest.mark.usefixtures("testmongoclient")
def test_failed_createprovenanceinformation(workspace, requests_mock):
    """Test provenance event creation failure.

    One of the provenance events of the dataset is invalid, which should
    cause exception.

    :param workspace: Test workspace directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Mock metax. Create a dataset with invalid provenance metadata.
    provenance = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
    del provenance["preservation_event"]
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['research_dataset']['provenance'] = [provenance]
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Run task
    with pytest.raises(
        InvalidDatasetMetadataError,
        match="Provenance metadata does not have key 'preservation_event'"
    ):
        task.run()

    # Task should not be complete
    assert not task.complete()
    assert not (workspace / 'preservation/mets.xml').exists()


@pytest.mark.parametrize(
    ['provenance_data', 'premis_id'],
    [
        [
            tests.metax_data.datasets.BASE_PROVENANCE,
            '6fc8a863bb6ed3cee2b1e853aa38d2db'
        ],
        [
            tests.metax_data.datasets.QVAIN_PROVENANCE,
            '56e8efc54b66b68319a3db79e6038eb3'
        ]
    ]
)
def test_create_premis_events(
    workspace, requests_mock, provenance_data, premis_id
):
    """Test creating METS with provenance event.

    Output XML file should be produced and it should contain some
    specified elements.

    :param workspace: Temporary directory
    :param requests_mock: HTTP request mocker
    :param provenance_data: The data used for creating provenance events
    :param premis_id: The id created for the PREMIS event
    """
    # Mock metax. Create a dataset with one provenance event
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    dataset["research_dataset"]["provenance"] = [provenance_data]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Run task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()

    # Find the digiprovMD element of provenance event from METS document
    mets = lxml.etree.parse(str(workspace / 'preservation' / 'mets.xml'))
    digiprovmd = mets.xpath(
        f"/mets:mets/mets:amdSec/mets:digiprovMD[@ID='_{premis_id}']",
        namespaces=NAMESPACES
    )[0]

    # Check that created  digiprovMD element contains correct elements.
    elements = digiprovmd.xpath('mets:mdWrap', namespaces=NAMESPACES)
    assert elements[0].attrib["MDTYPE"] == "PREMIS:EVENT"
    assert elements[0].attrib["MDTYPEVERSION"] == "2.3"

    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event/premis:eventIdentifier'
        '/premis:eventIdentifierType',
        namespaces=NAMESPACES
    )
    assert elements[0].text == "UUID"

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

    # Outcome should be "unknown" if missing
    elements = digiprovmd.xpath(
        'mets:mdWrap/mets:xmlData/premis:event'
        '/premis:eventOutcomeInformation/premis:eventOutcome',
        namespaces=NAMESPACES)
    if "event_outcome" in provenance_data:
        assert elements[0].text == "outcome"
    else:
        assert elements[0].text == "unknown"

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


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata(workspace, requests_mock):
    """Test descriptive metadata creation.

    Datacite XML should be imported to dmdSec of METS. DmdSec should be
    referenced in structure maps. Premis event should be created for
    datacite import.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock Metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Init and runtask
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()
    assert task.complete()

    # Check that METS document contains correct elements.
    mets = etree.parse(str(workspace / 'preservation/mets.xml'))

    # The mdWrap element should contain the datacite metadata
    dmdsec = mets.xpath('/mets:mets//mets:dmdSec', namespaces=NAMESPACES)[0]
    mdwrap = dmdsec.xpath('mets:mdWrap', namespaces=NAMESPACES)
    assert mdwrap[0].attrib["OTHERMDTYPE"] == "DATACITE"
    assert mdwrap[0].attrib["MDTYPEVERSION"] == "4.1"
    mets_datacite = mdwrap[0].xpath('mets:xmlData/datacite:resource',
                                    namespaces=NAMESPACES)[0]

    # Compare datacite metadata in METS file to the original datacite
    # metadata retrieved from metax. First rip the datacite from METS
    # and lean up extra namespaces.
    mets_datacite = etree.fromstring(etree.tostring(mets_datacite))
    etree.cleanup_namespaces(mets_datacite)
    # Compare XMLs. The string presertations should be indentical
    metax_datacite = tests.metax_data.datasets.BASE_DATACITE.getroot()
    assert etree.tostring(mets_datacite) == etree.tostring(metax_datacite)

    # Check that descriptive metadata is referenced in both structMaps
    # (Fairdata-physical and Fairdata-logical)
    dmdsec_id = dmdsec.attrib["ID"]
    structmaps = mets.xpath("/mets:mets/mets:structMap", namespaces=NAMESPACES)
    assert len(structmaps) == 2
    for structmap in structmaps:
        structmap_div = structmap.xpath("mets:div", namespaces=NAMESPACES)[0]
        assert structmap_div.attrib["DMDID"] == dmdsec_id

    # Check that premis event is created for descriptive metadata import
    extraction_events \
        = mets.xpath('//premis:event[premis:eventType="metadata extraction"]',
                     namespaces=NAMESPACES)
    assert len(extraction_events) == 1
    event_detail = extraction_events[0].xpath('premis:eventDetail',
                                              namespaces=NAMESPACES)[0]
    assert event_detail.text \
        == 'Descriptive metadata import from external source'


@pytest.mark.usefixtures('testmongoclient')
def test_createdescriptivemetadata_invalid_datacite(workspace, requests_mock):
    """Test importing invalid Datacite XML.

    The METS creation should fail when datacite metadata is not valid.

    :param workspace: Test workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Create dataset that contains invalid datacite metadata
    datacite = etree.Element("{foo}bar")
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  datacite=datacite)

    # Init task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # The method should fail
    with pytest.raises(TypeError, match='Invalid namespace: foo'):
        task.run()

    assert not task.complete()


def xml2simpledict(element):
    """Convert XML element to simple dict.

    :param element: XML element
    :returns: Dictionary
    """
    # Convert XML to dictionary. Expand namespace prefixes.
    dictionary = xmltodict.parse(lxml.etree.tostring(element),
                                 process_namespaces=True,
                                 dict_constructor=dict)
    # Remove namespace elements
    for key in dictionary:
        if '@xmlns' in dictionary[key]:
            del dictionary[key]['@xmlns']

    return dictionary


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_ok(workspace, requests_mock):
    """Test technical metadata creation.

    Create METS for a dataset that contains one TIFF file. Test that
    METS contains PREMIS object and NISOIMG metadata that are linked to
    structure map. Test that PREMIS agent metadata is created for tools
    of file-scraper.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[TIFF_FILE])

    # Create workspace that already contains the dataset files
    dataset_files_parent = workspace / 'metadata_generation'
    tiff_path = 'dataset_files/' + TIFF_FILE['file_path']
    (dataset_files_parent / tiff_path).parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/valid_tiff.tiff',
                dataset_files_parent / tiff_path)

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Read created METS
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # There should be one file in fileSec
    file_elements = mets.xpath('//mets:file', namespaces=NAMESPACES)
    assert len(file_elements) == 1

    # METS should contain two techMD elements, one for PREMIS:OBJECT and
    # one for NISOIMG. Both of them should be linked to a to the file in
    # fileSec.
    techmd_elements = mets.xpath("//mets:techMD", namespaces=NAMESPACES)
    assert len(techmd_elements) == 2
    for element in techmd_elements:
        assert element.attrib['ID'] in file_elements[0].attrib["ADMID"]

    # Check that the PREMIS object element has desired properties
    premis_object_element \
        = mets.xpath("//premis:object", namespaces=NAMESPACES)[0]
    assert premis_object_element.xpath(
        "//premis:object/@*", namespaces=NAMESPACES
    )[0] == 'premis:file'
    assert premis_object_element.xpath(
        "//premis:formatName", namespaces=NAMESPACES
    )[0].text == 'image/tiff'
    assert premis_object_element.xpath(
        "//premis:formatVersion",
        namespaces=NAMESPACES
    )[0].text == '6.0'

    # METS should contain PREMIS agents for variety if detectors of
    # file-scraper, for example FidoDetector, MagicDetetector,
    # SiardDetector etc.
    premis_agent_names = [
        element.text
        for element
        in mets.xpath("//premis:agentName", namespaces=NAMESPACES)
    ]
    for agent in "FidoDetector", "MagicDetector", "SiardDetector":
        assert any(premis_agent_name.startswith(agent)
                   for premis_agent_name
                   in premis_agent_names)

    # Compare MIX metadata in METS file to original MIX metadata in
    # Metax
    mix = mets.xpath('//mix:mix', namespaces=NAMESPACES)[0]
    original_mix = lxml.etree.parse("tests/data/mix_sample_tiff.xml")
    original_mix = original_mix.xpath(
        "/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/mets:xmlData/*",
        namespaces=NAMESPACES
    )[0]
    assert xml2simpledict(mix) == xml2simpledict(original_mix)


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_multiple_metadata_documents(workspace, requests_mock):
    """Test techmd creation for a file with multiple streams.

    Create a METS document for a dataset that contains a Matroska file
    which contains two similar audio streams and one video streams.
    PREMIS objects should be created for all streams and the container.
    One AudioMD metadata should be created for the audio streams, and
    one VideoMD metadata should be created for the video stream.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[MKV_FILE])

    # Create workspace that already contains the dataset files
    mkv_path = workspace / 'metadata_generation/dataset_files' \
        / MKV_FILE['file_path']
    mkv_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/video_ffv1.mkv', mkv_path)

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Read created mets
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # METS should contain four PREMIS objects in total, two for audio
    # streams, one for video stream, and one for container.
    format_names = [
        element.text for element
        in mets.xpath('//premis:formatName', namespaces=NAMESPACES)
    ]
    assert format_names.count("audio/flac") == 2
    assert format_names.count("video/x-ffv") == 1
    assert format_names.count("video/x-matroska") == 1

    # There should be only one techmMD element for audio streams
    audiomds = mets.xpath("//mets:techMD/mets:mdWrap[@OTHERMDTYPE='AudioMD']",
                          namespaces=NAMESPACES)
    assert len(audiomds) == 1
    assert audiomds[0].xpath(".//audiomd:codecName",
                             namespaces=NAMESPACES)[0].text == "FLAC"

    # There should be a techmMD element for the video stream
    videomds = mets.xpath("//mets:techMD/mets:mdWrap[@OTHERMDTYPE='VideoMD']",
                          namespaces=NAMESPACES)
    assert len(videomds) == 1
    assert videomds[0].xpath(".//videomd:codecName",
                             namespaces=NAMESPACES)[0].text == "FFV1"


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_incomplete_file_characteristics(workspace,
                                                       requests_mock):
    """Test techmd creation for a file without all the necessary file
    characteristics.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    """
    # TODO: This test probably is not necessary, and could be removed.
    # The file_characteristics_extension is created by packaging
    # service, so it should always be valid.
    tiff_file_incomplete = copy.deepcopy(TIFF_FILE)
    del (tiff_file_incomplete["file_characteristics_extension"]["streams"]
         [0]["bps_value"])
    # Mock metax
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[tiff_file_incomplete])

    # Create workspace that already contains the dataset files
    tiff_path = workspace / "metadata_generation" / "dataset_files" \
        / TIFF_FILE["file_path"]
    tiff_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/valid_tiff.tiff', tiff_path)

    # Init task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Run task
    with pytest.raises(KeyError) as exc:
        task.run()

    assert "bps_value" in str(exc.value)


@pytest.mark.usefixtures()
def test_create_techmd_without_charset(workspace, requests_mock):
    """Test techmd creation for files without defined charset.

    UTF-8 should be used as default charset, if charset is not defined.

    :param workspace: Test workspace directory
    :param requests_mock: HTTP requeset mocker
    """
    text_file = copy.deepcopy(TXT_FILE)
    del text_file['file_characteristics']['encoding']
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    tests.utils.add_metax_dataset(requests_mock,
                                  dataset=dataset,
                                  files=[text_file])

    # Create workspace that contains a textfile
    dataset_files = workspace / "metadata_generation" / "dataset_files"
    text_file_path = dataset_files / "path" / "to" / "file"
    text_file_path.parent.mkdir(parents=True)
    text_file_path.write_text("foo")

    # Init and run task
    task = CreateMets(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()

    # Read METS
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))

    # If charset is not defined the siptools.import_objects default
    # value is used. Siptools recognizes ASCII text files as UTF-8 text
    # files.
    format_name = mets.xpath("//premis:formatName",
                             namespaces=NAMESPACES)[0].text
    assert format_name == 'text/plain; charset=UTF-8'


@pytest.mark.parametrize(('algorithm', 'hash_function', 'expected'),
                         [('md5', hashlib.md5, 'MD5'),
                          ('sha2', hashlib.sha224, 'SHA-224'),
                          ('sha2', hashlib.sha256, 'SHA-256'),
                          ('sha2', hashlib.sha384, 'SHA-384'),
                          ('sha2', hashlib.sha512, 'SHA-512')])
def test_algorithm_name_valid_input(algorithm, hash_function, expected):
    """Test ``algorithm_name`` function with valid inputs."""
    assert algorithm_name(algorithm,
                          hash_function(b'foo').hexdigest()) == expected


@pytest.mark.parametrize(('algorithm', 'value', 'expected_exception'),
                         [('foo', 'bar', UnboundLocalError),
                          ('sha2', 'foobar', KeyError)])
def test_algorithm_name_invalid_input(algorithm, value, expected_exception):
    """Test ``algorithm_name`` function with invalid inputs."""
    with pytest.raises(expected_exception):
        algorithm_name(algorithm, value)


@pytest.mark.parametrize(
    'provenance_descriptions',
    (
        # No provenance events
        [],
        # One provenance events
        ['provenance1'],
        # Multiple provenance events
        ['provenance1', 'provenance2'])
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(workspace, requests_mock,
                             provenance_descriptions):
    """Test physical structure map creation.

    Creates structMap and fileSec for a dataset that contains some
    files in a directory structure. Checks that structMap and fileSec
    contain expected elements. Checks that structMap is linked to
    provenance event metadata.

    :param workspace: Temporary workspace fixture
    :param requests_mock: HTTP request mocker
    :param provenance_descriptions: Descriptions of provenance events
    """
    # Create dataset that contains three text files
    files = []
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE) for i in range(3)]
    files[0]["file_path"] = "/file1"
    files[1]["file_path"] = "/file2"
    files[2]["file_path"] = "/subdirectory/file3"
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    # Add provenance events to dataset
    if provenance_descriptions:
        dataset["research_dataset"]["provenance"] = []
        for provenance_description in provenance_descriptions:
            provenance \
                = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
            provenance["description"]["en"] = provenance_description
            dataset["research_dataset"]["provenance"].append(provenance)
    tests.utils.add_metax_dataset(requests_mock=requests_mock,
                                  dataset=dataset,
                                  files=files)

    # Create files in workspace
    dataset_files = workspace / "metadata_generation/dataset_files"
    subdirectory = dataset_files / "subdirectory"
    subdirectory.mkdir(parents=True)
    (dataset_files / "file1").write_text("foo")
    (dataset_files / "file2").write_text("bar")
    (subdirectory / "file3").write_text("baz")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

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
    assert set(files) == {'file://dataset_files/file1',
                          'file://dataset_files/file2',
                          'file://dataset_files/subdirectory/file3'}

    # Validate the "Fairdata-physical" structMap in METS.
    structmap = mets.xpath("//mets:structMap[@TYPE='Fairdata-physical']",
                           namespaces=NAMESPACES)[0]
    assert structmap.xpath(
        "mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'directory'
    assert structmap.xpath(
        "mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'dataset_files'
    assert structmap.xpath(
        "mets:div/mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'subdirectory'
    # Two files should be found in data directory
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 2
    # One file should be found in subdirectory of data directory
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:div'
        '/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 1

    # Premis events should be created for each provenance event found
    # in dataset metadata
    premis_event_descriptions = [
        element.text for element
        in mets.xpath("//premis:eventDetail", namespaces=NAMESPACES)
    ]
    for provenance_description in provenance_descriptions:
        assert provenance_description in premis_event_descriptions

    # Structure map should be linked to all digiprovMD elements
    digiprovmd_ids = mets.xpath("//mets:digiprovMD/@ID", namespaces=NAMESPACES)
    structmap_admids = structmap.xpath("mets:div/@ADMID",
                                       namespaces=NAMESPACES)[0]
    for digiprovmd_id in digiprovmd_ids:
        assert digiprovmd_id in structmap_admids


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_without_directories(workspace, requests_mock):
    """Test creating structmap for dataset without directories.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Create a dataset that contains only one file
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE)]
    files[0]["file_path"] = "/file1"
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock=requests_mock,
                                  dataset=dataset,
                                  files=files)

    # Create the file in "dataset_files"
    dataset_files = workspace / "metadata_generation/dataset_files"
    dataset_files.mkdir(parents=True)
    (dataset_files / "file1").write_text("foo")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Check the structmap element
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))
    structmap = mets.xpath("//mets:structMap[@TYPE='Fairdata-physical']",
                           namespaces=NAMESPACES)[0]
    assert structmap.xpath("mets:div/@TYPE",
                           namespaces=NAMESPACES)[0] == 'directory'
    assert structmap.xpath("mets:div/mets:div/@TYPE",
                           namespaces=NAMESPACES)[0] == 'dataset_files'
    assert len(structmap.xpath('mets:div/mets:div/mets:fptr/@FILEID',
                               namespaces=NAMESPACES)) == 1


@pytest.mark.usefixtures('testmongoclient')
def test_filesec_othermd(workspace, requests_mock):
    """Test creating structmap for dataset with othermd metadata.

    Creates METS for a dataset that contains image file. MIX metadata
    should be created, and the file element in fileSec should be linked
    to to MIX metadata.

    :param workspace: Temporary packaging directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Create a dataset that contains an image file
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    files = [copy.deepcopy(tests.metax_data.files.TIFF_FILE)]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Copy image file to test workspace
    filepath \
        = workspace / "metadata_generation/dataset_files/path/to/file.tiff"
    filepath.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/valid_tiff.tiff", filepath)

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Filesec should contain one file
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    file_elements = mets.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file',
        namespaces=NAMESPACES
    )
    assert len(file_elements) == 1

    # The file element should contain link to MIX metadata
    mix_techmd_element = mets.xpath(
        "//mets:mdWrap[@MDTYPE='NISOIMG']/parent::mets:techMD",
        namespaces=NAMESPACES
    )[0]
    assert mix_techmd_element.attrib["ID"] in file_elements[0].attrib['ADMID']


@pytest.mark.parametrize(
    ("events", "provenance_is_from_qvain"),
    [
        (
            [], False
        ),
        (
            ['creation'], False
        ),
        (
            ['creation', 'foobar'], False
        ),
        (
            ['creation'], True
        ),
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_logical_structmap_ok(
    workspace, requests_mock, events, provenance_is_from_qvain
):
    """Test creating logical structure map.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param events: List of provenance events in dataset
    :param provenance_is_from_qvain: Choose type provenance event
    """
    provenance_description = "This is a string"
    # Create a dataset
    # Dataset contains two files
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE),
             copy.deepcopy(tests.metax_data.files.TXT_FILE)]
    files[0]['file_path'] = 'files/file1'
    files[1]['file_path'] = 'files/file2'
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    # Dataset contans zero or more events
    dataset["research_dataset"]["provenance"] = []
    for event in events:
        # Provenances made in Qvain have 'lifecycle_event' instead of
        # 'provenance_event'
        if provenance_is_from_qvain:
            provenance = copy.deepcopy(
                tests.metax_data.datasets.QVAIN_PROVENANCE
            )
            provenance["lifecycle_event"]["pref_label"]["en"] = event
            provenance["title"]["en"] = provenance_description
        else:
            provenance = copy.deepcopy(
                tests.metax_data.datasets.BASE_PROVENANCE
            )
            provenance["preservation_event"]["pref_label"]["en"] = event
            provenance["description"]["en"] = provenance_description
        dataset["research_dataset"]["provenance"].append(provenance)
    if not dataset["research_dataset"]["provenance"]:
        del dataset["research_dataset"]["provenance"]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Create workspace that already contains dataset files
    file_directory = workspace / 'metadata_generation/dataset_files/files'
    file_directory.mkdir(parents=True)
    (file_directory / "file1").write_text("foo")
    (file_directory / "file2").write_text("bar")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Validate logical Fairdata-logical structure map
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    structmap = mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="Fairdata-logical"]',
        namespaces=NAMESPACES
    )[0]
    assert structmap.xpath('mets:div',
                           namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "logical"

    # Find the digiprovMD elements that were created for provenance
    # events
    provenance_digiprovmd_elements = mets.xpath(
        f"//*[premis:eventDetail='{provenance_description}']"
        "/ancestor::mets:digiprovMD",
        namespaces=NAMESPACES
    )

    # There should be one digiprovMD element per provenance event
    assert len(provenance_digiprovmd_elements) == len(events)

    # Each provenance event should be linked to logical structMap
    for element in provenance_digiprovmd_elements:
        assert element.attrib['ID'] in structmap.xpath(
            'mets:div',
            namespaces=NAMESPACES
        )[0].attrib['ADMID'].split()

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


def test_get_dirpath_dict(requests_mock):
    """Test that get_dirpath_dict returns the correct dictionary.

    The dictionary maps dirpath to use_category.

    :param requests_mock: Mocker object
    """
    requests_mock.get(
        "https://metaksi/rest/v2/directories/1",
        json={
            "identifier": "1",
            "directory_path": "/"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v2/directories/2",
        json={
            "identifier": "2",
            "directory_path": "/test"
        }
    )

    metax_client = Metax("https://metaksi", "test", "test")
    dataset_metadata = {
        "research_dataset": {
            "directories": [
                {
                    "identifier": "1",
                    "use_category": {"pref_label": {"en": "rootdir"}}
                },
                {
                    "identifier": "2",
                    "use_category": {"pref_label": {"en": "testdir"}}
                }
            ]
        }
    }

    assert get_dirpath_dict(metax_client, dataset_metadata) == {
        "/": {"pref_label": {"en": "rootdir"}},
        "/test": {"pref_label": {"en": "testdir"}}
    }


def test_get_dirpath_dict_no_directories():
    """Test get_dirpath_dict function with dataset without directories.

    The function should return an empty dict when no directories are
    defined in the research_dataset.
    """
    metax_client = Metax("https://metaksi", "test", "test")
    assert not get_dirpath_dict(metax_client, {"research_dataset": {}})


def test_find_dir_use_category():
    """Test that find_dir_use_category returns the correct label."""
    dirpath_dict = {
        "/test1": {"pref_label": {"en": "testdir1"}},
        "/test2": {"pref_label": {"en": "testdir2"}}
    }
    languages = ["en"]

    # Straightforward cases
    assert find_dir_use_category("/test1", dirpath_dict, languages) \
        == "testdir1"
    assert find_dir_use_category("/test2", dirpath_dict, languages) \
        == "testdir2"

    # Closest parent that matches
    assert find_dir_use_category(
        "/test1/test", dirpath_dict, languages
    ) == "testdir1"

    # No matches
    assert not find_dir_use_category("/", dirpath_dict, languages)
    assert not find_dir_use_category("/test3", dirpath_dict, languages)

    # No directories were found in the research_dataset
    assert not find_dir_use_category("/test", {}, languages)

    # Match to root
    assert find_dir_use_category(
        "/",
        {"/": {"pref_label": {"en": "root"}}},
        languages
    ) == "root"
