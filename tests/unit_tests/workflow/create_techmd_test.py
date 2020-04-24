"""Test the :mod:`siptools_research.workflow.create_techmd` module"""

import os
import shutil
import hashlib
import glob
import lxml.etree
import pytest

from metax_access import MetaxError

from siptools.xml.mets import NAMESPACES
from siptools.mdcreator import read_md_references
import tests.conftest
from siptools_research.workflow.create_techmd import (CreateTechnicalMetadata,
                                                      algorithm_name)


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_create_techmd_ok(testpath, requests_mock):
    """Test the workflow task CreateTechnicalMetadata module.

    :param testpath: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:5/xml", json=[])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:6/xml", json=[])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:7/xml", json=[])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:999/xml", json=[])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:888/xml",
                      json=["http://www.loc.gov/mix/v20"])

    with open("tests/data/mix_sample_jpeg.xml", "rb") as mix:
        requests_mock.get(
            "https://metaksi/rest/v1/files/pid:urn:888/xml"
            "?namespace=http://www.loc.gov/mix/v20",
            content=mix.read()
        )

    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree('tests/data/sample_dataset_directories/project_x',
                    os.path.join(sipdirectory, 'project_x'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id='create_techmd_test_dataset',
                                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    found_refs = 0
    mix_refs = read_md_references(sipdirectory,
                                  'create-mix-md-references.json')
    found_refs += len(mix_refs)
    amd_refs = read_md_references(
        sipdirectory, 'import-object-md-references.json')
    found_refs += len(amd_refs)
    assert found_refs == 6
    for path in amd_refs:
        for amd_ref in amd_refs[path]['md_ids']:
            if amd_ref[1:] != '1b2eecde68d99171f70613f14cf21f49':
                assert os.path.isfile(os.path.join(
                    sipdirectory,
                    amd_ref[1:] + '-PREMIS%3AOBJECT-amd.xml'
                    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        '1b2eecde68d99171f70613f14cf21f49-NISOIMG-amd.xml'
    ))
    # Check that one of the PREMIS techMD files has desired properties
    output_file = os.path.join(
        sipdirectory, amd_refs['project_x/some/path/file_name_5'][
            'md_ids'][0][1:] + '-PREMIS%3AOBJECT-amd.xml'
    )
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] \
        == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'text/html; charset=UTF-8'
    assert root.xpath("//premis:formatVersion",
                      namespaces=NAMESPACES)[0].text == '5.0'

    # Check that the NISOIMG techMD file has desired properties
    output_file = os.path.join(
        sipdirectory,
        '1b2eecde68d99171f70613f14cf21f49-NISOIMG-amd.xml'
    )
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//mix:mix", namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicDigitalObjectInformation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/"
                          "mix:BasicImageCharacteristics",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/"
                          "mix:BasicImageCharacteristics/"
                          "mix:PhotometricInterpretation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/"
                          "mix:SpecialFormatCharacteristics",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:ImageAssessmentMetadata",
                          namespaces=NAMESPACES)) == 1

    # Check that target file is created
    with open(os.path.join(workspace, 'task-create-technical-'
                           'metadata.finished')) as open_file:
        assert 'Dataset id=create_techmd_test_dataset' in open_file.read()

    # Check that one event file is created and only one reference
    event_refs = read_md_references(sipdirectory,
                                    'premis-event-md-references.json')
    assert len(event_refs) == 1
    assert event_refs['.']
    assert len(glob.glob1(sipdirectory, '*-PREMIS%3AEVENT-amd.xml')) == 1


@pytest.mark.usefixtures('mock_metax_access')
# pylint: disable=invalid-name
def test_create_techmd_without_charset(testpath, requests_mock):
    """Test the task with dataset that has files without defined charset

    :param requests_mock: Mocker object
    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.get(
        "https://metaksi/rest/v1/files/pid:urn:create_techmd_3/xml",
        json=[]
    )

    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree('tests/data/sample_dataset_directories/project_x',
                    os.path.join(sipdirectory, 'project_x'))

    # Init and run task
    task = CreateTechnicalMetadata(
        workspace=workspace,
        dataset_id='create_techmd_test_dataset_charset_not_defined',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    task.run()

    amd_refs = read_md_references(
        sipdirectory, 'import-object-md-references.json')
    assert len(amd_refs) == 1
    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        amd_refs['project_x/some/path/file_name_5']['md_ids'][0][1:] + '-PREMIS%3AOBJECT-amd.xml'
    )
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    # If charset is not defined the siptools.import_objects default value is
    # used. Siptools recognizes ASCII text files as UTF-8 text files.
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'text/html; charset=UTF-8'


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_xml_metadata_file_missing(testpath, requests_mock):
    """Test the workflow task CreateTechnicalMetadata module when XML
    metadata for a file is missing. Behavior not specified yet. Currently
    throws an error.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:8/xml",
                      json=["http://www.loc.gov/mix/v20"])
    requests_mock.get("https://metaksi/rest/v1/files/pid:urn:8/xml"
                      "?namespace=http://www.loc.gov/mix/v20",
                      status_code=404)

    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree(
        'tests/data/sample_dataset_directories/project_xml_metadata_missing',
        os.path.join(sipdirectory, 'project_xml_metadata_missing')
    )

    # Init task
    task = CreateTechnicalMetadata(
        workspace=workspace,
        dataset_id="create_techmd_test_dataset_xml_metadata_missing",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task.
    msg = "Could not retrieve additional metadata XML for dataset pid:urn:8"
    with pytest.raises(MetaxError) as exc:
        task.run()

    assert msg in str(exc.value)
    assert not task.complete()


def test_algorithm_name():
    """Test ``algorithm_name`` function with valid and invalid inputs.

    :returns: ``None``
    """

    # Valid input
    assert algorithm_name('md5', hashlib.md5(b'foo').hexdigest()) == 'MD5'
    assert algorithm_name('sha2', hashlib.sha224(b'foo').hexdigest()) \
        == 'SHA-224'
    assert algorithm_name('sha2', hashlib.sha256(b'foo').hexdigest()) \
        == 'SHA-256'
    assert algorithm_name('sha2', hashlib.sha384(b'foo').hexdigest()) \
        == 'SHA-384'
    assert algorithm_name('sha2', hashlib.sha512(b'foo').hexdigest()) \
        == 'SHA-512'

    # invalid algorithm name
    with pytest.raises(UnboundLocalError):
        algorithm_name('foo', 'bar')

    # invalid value length
    with pytest.raises(KeyError):
        algorithm_name('sha2', 'foobar')
