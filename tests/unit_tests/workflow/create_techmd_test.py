"""Test the :mod:`siptools_research.workflow.create_techmd` module."""

import copy
import hashlib
import json
import shutil
from collections import defaultdict
from pathlib import Path

import lxml.etree
import pytest
from siptools.utils import read_md_references
from siptools.xml.mets import NAMESPACES
import xmltodict

from tests.metax_data.files import TIFF_FILE, MKV_FILE, TXT_FILE
import tests.utils
from siptools_research.workflow.create_techmd import (CreateTechnicalMetadata,
                                                      algorithm_name)


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
    """Test the workflow task CreateTechnicalMetadata module.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock, files=[TIFF_FILE])

    # Create workspace that already contains the dataset files
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir()
    tiff_path = Path('dataset_files') / TIFF_FILE['file_path']
    (workspace / tiff_path).parent.mkdir(parents=True)

    shutil.copy(
        'tests/data/sample_files/valid_tiff.tiff',
        workspace / tiff_path
    )

    # Init task
    task = CreateTechnicalMetadata(workspace=str(workspace),
                                   dataset_id='dataset_identifier',
                                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Premis object references should be written to file.
    premis_object_references \
        = read_md_references(sipdirectory, 'import-object-md-references.jsonl')
    assert len(premis_object_references) == 1
    assert len(premis_object_references[str(tiff_path)]['md_ids']) == 1

    # Check that the PREMIS object file has desired properties
    premis_object_identifier \
        = (premis_object_references[str(tiff_path)]['md_ids'][0][1:])
    premis_object_file = f'{premis_object_identifier}-PREMIS%3AOBJECT-amd.xml'
    premis_object_xml = lxml.etree.parse(
        str(sipdirectory / premis_object_file)
    )
    assert len(premis_object_xml.xpath('//mets:amdSec',
                                       namespaces=NAMESPACES)) == 1
    assert len(premis_object_xml.xpath("//premis:object",
                                       namespaces=NAMESPACES)) == 1
    assert premis_object_xml.xpath(
        "//premis:object/@*", namespaces=NAMESPACES
    )[0] == 'premis:file'
    assert premis_object_xml.xpath(
        "//premis:formatName", namespaces=NAMESPACES
    )[0].text == 'image/tiff'
    assert premis_object_xml.xpath("//premis:formatVersion",
                                   namespaces=NAMESPACES)[0].text == '6.0'

    # The file properties of premis object should written to json file
    file_properties_file = f'{premis_object_identifier}-scraper.json'
    file_properties = json.loads(
        (sipdirectory / file_properties_file).read_bytes()
    )
    assert file_properties['0']['mimetype'] == 'image/tiff'
    assert file_properties['0']['version'] == '6.0'

    # One premis event file should be created
    premis_event_files = [
        file_.name for file_ in sipdirectory.iterdir()
        if file_.name.endswith('-PREMIS%3AEVENT-amd.xml')
    ]
    assert len(premis_event_files) == 1
    premis_event_id \
        = premis_event_files[0].rsplit('-PREMIS%3AEVENT-amd.xml')[0]

    # Some premis agent files should be created
    premis_references = read_md_references(
        str(workspace), 'create-technical-metadata.jsonl'
    )
    premis_agent_files = [f'{id_[1:]}-PREMIS%3AAGENT-amd.xml'
                          for id_ in premis_references['.']['md_ids']
                          if id_[1:] != premis_event_id]
    assert len(premis_agent_files) == 10
    for file_ in premis_agent_files:
        assert (sipdirectory / file_).is_file()

    # MIX references should be written to file
    mix_references = read_md_references(
        str(sipdirectory), 'create-mix-md-references.jsonl'
    )
    assert len(mix_references) == 1
    assert len(mix_references[str(tiff_path)]["md_ids"]) == 1
    assert mix_references[str(tiff_path)]["md_ids"][0] \
        == '_dd0f489d6e47cc2dca598beb608cc78d'

    # Compare MIX metadata in techMD file to original MIX metadata in
    # Metax
    mets = lxml.etree.parse(
        str(sipdirectory / 'dd0f489d6e47cc2dca598beb608cc78d-NISOIMG-amd.xml')
    )
    mdwrap = mets.xpath('/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap',
                        namespaces=NAMESPACES)[0]
    mix = mdwrap.xpath('mets:xmlData/mix:mix', namespaces=NAMESPACES)[0]
    original_mix = lxml.etree.fromstring(
        Path("tests/data/mix_sample_tiff.xml").read_bytes()
    )

    original_mix = original_mix.xpath(
        "/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/mets:xmlData/*",
        namespaces=NAMESPACES
    )[0]
    assert xml2simpledict(mix) == xml2simpledict(original_mix)

    # SIP directory should contain all technical metadata and related
    # files
    files = {path.name for path in sipdirectory.iterdir()}
    assert files \
        == set(['import-object-md-references.jsonl',
                premis_object_file,
                file_properties_file,
                'create-mix-md-references.jsonl',
                'dd0f489d6e47cc2dca598beb608cc78d-NISOIMG-amd.xml']
               + premis_agent_files
               + premis_event_files)


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_multiple_metadata_documents(
        workspace, requests_mock):
    """Test techmd creation for a file with multiple streams.

    Multiple technical metadata documents should be created.
    """
    tests.utils.add_metax_dataset(requests_mock, files=[MKV_FILE])

    # Create workspace that already contains the dataset files
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir()
    dataset_files = workspace / 'dataset_files'
    mkv_path = dataset_files / MKV_FILE['file_path']
    mkv_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/video_ffv1.mkv', mkv_path)

    # Init task
    task = CreateTechnicalMetadata(workspace=str(workspace),
                                   dataset_id='dataset_identifier',
                                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    premis_object_paths = sipdirectory.glob("*-PREMIS%3AOBJECT-amd.xml")
    premis_objects = []
    premis_objects = [
        lxml.etree.fromstring(path.read_bytes())
        for path in premis_object_paths
    ]

    mime_type_count = defaultdict(int)

    for premis_object in premis_objects:
        file_type = premis_object.xpath(
            "//premis:format/premis:formatDesignation/premis:formatName",
            namespaces=NAMESPACES
        )[0].text
        mime_type_count[file_type] += 1

    # Four PREMIS objects in total, two for audio streams
    assert mime_type_count["audio/flac"] == 2
    assert mime_type_count["video/x-ffv"] == 1
    assert mime_type_count["video/x-matroska"] == 1

    videomd_path = next(sipdirectory.glob("*VideoMD-amd.xml"))
    audiomd_path = next(sipdirectory.glob("*AudioMD-amd.xml"))

    videomd = lxml.etree.fromstring(videomd_path.read_bytes())
    audiomd = lxml.etree.fromstring(audiomd_path.read_bytes())

    assert audiomd.xpath(
        "//audiomd:codecName", namespaces=NAMESPACES
    )[0].text == "FLAC"

    assert videomd.xpath(
        "//videomd:codecName", namespaces=NAMESPACES
    )[0].text == "FFV1"


@pytest.mark.usefixtures('testmongoclient')
def test_create_techmd_incomplete_file_characteristics(
        workspace, requests_mock):
    """Test techmd creation for a file without all the necessary file
    characteristics.
    """
    tiff_file_incomplete = copy.deepcopy(TIFF_FILE)
    del (tiff_file_incomplete["file_characteristics_extension"]["streams"]
         [0]["bps_value"])
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock,
                                  files=[tiff_file_incomplete])

    # Create workspace that already contains the dataset files
    sipdirectory = workspace / "sip-in-progress"
    sipdirectory.mkdir()
    tiff_path = workspace / "dataset_files" / TIFF_FILE["file_path"]
    tiff_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/valid_tiff.tiff', tiff_path)

    # Init task
    task = CreateTechnicalMetadata(workspace=str(workspace),
                                   dataset_id='dataset_identifier',
                                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Run task
    with pytest.raises(KeyError) as exc:
        task.run()

    assert "bps_value" in str(exc.value)


@pytest.mark.usefixtures()
# pylint: disable=invalid-name
def test_create_techmd_without_charset(workspace, requests_mock):
    """Test techmd creation for files without defined charset.

    :param workspace: Test workspace directory
    :returns: ``None``
    """
    text_file = copy.deepcopy(TXT_FILE)
    del text_file['file_characteristics']['encoding']
    tests.utils.add_metax_dataset(requests_mock, files=[text_file])

    # Create workspace that contains a textfile
    sipdirectory = workspace / 'sip-in-progress'
    sipdirectory.mkdir(parents=True)
    dataset_files = workspace / "dataset_files"
    text_file_path = dataset_files / "path" / "to" / "file"
    text_file_path.parent.mkdir(parents=True)

    text_file_path.write_text("foo")

    # Init and run task
    task = CreateTechnicalMetadata(
        workspace=str(workspace),
        dataset_id='dataset_identifier',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()

    # Metadata reference file and premis object XML file should be
    # created in SIP directory
    amd_refs = read_md_references(str(sipdirectory),
                                  'import-object-md-references.jsonl')
    assert len(amd_refs) == 1
    amd_id = amd_refs['dataset_files/path/to/file']['md_ids'][0][1:]
    premis_object_xml = lxml.etree.parse(
        str(sipdirectory / f'{amd_id}-PREMIS%3AOBJECT-amd.xml')
    )

    # If charset is not defined the siptools.import_objects default
    # value is used. Siptools recognizes ASCII text files as UTF-8 text
    # files.
    format_name = premis_object_xml.xpath("//premis:formatName",
                                          namespaces=NAMESPACES)[0].text
    assert format_name == 'text/plain; charset=UTF-8'


@pytest.mark.parametrize(('algorithm', 'hash_function', 'expected'),
                         [('md5', hashlib.md5, 'MD5'),
                          ('sha2', hashlib.sha224, 'SHA-224'),
                          ('sha2', hashlib.sha256, 'SHA-256'),
                          ('sha2', hashlib.sha384, 'SHA-384'),
                          ('sha2', hashlib.sha512, 'SHA-512')])
def test_algorithm_name_valid_input(algorithm, hash_function, expected):
    """Test ``algorithm_name`` function with valid inputs.

    :returns: ``None``
    """
    assert algorithm_name(algorithm,
                          hash_function(b'foo').hexdigest()) == expected


@pytest.mark.parametrize(('algorithm', 'value', 'expected_exception'),
                         [('foo', 'bar', UnboundLocalError),
                          ('sha2', 'foobar', KeyError)])
def test_algorithm_name_invalid_input(algorithm, value, expected_exception):
    """Test ``algortih_name`` function with invalid inputs.

    :returns: ``None``
    """
    with pytest.raises(expected_exception):
        algorithm_name(algorithm, value)
