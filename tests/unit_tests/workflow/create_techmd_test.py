"""Test the `create_technical_metdata` method of CreateMets class."""

import copy
import hashlib
import shutil

import lxml.etree
import pytest
from siptools.xml.mets import NAMESPACES
import xmltodict

from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TIFF_FILE, MKV_FILE, TXT_FILE
import tests.utils
from siptools_research.workflow.create_mets import (CreateMets, algorithm_name)


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
