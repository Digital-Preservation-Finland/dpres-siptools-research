"""Test the :mod:`siptools_research.workflow.create_techmd` module"""

import os
import shutil
import hashlib
import lxml.etree
import pytest

from metax_access import MetaxError

from siptools.xml.mets import NAMESPACES
import tests.conftest
from siptools_research.workflow.create_techmd import (CreateTechnicalMetadata,
                                                      algorithm_name)


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
def test_create_techmd_ok(testpath):
    """Test the workflow task CreateTechnicalMetadata module.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
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
    reference_file = os.path.join(
        sipdirectory, 'import-object-md-references.xml')
    xml = lxml.etree.parse(reference_file)
    amd_refs = xml.xpath('/mdReferences/mdReference')
    assert len(amd_refs) == 6
    for amd_ref in amd_refs:
        if amd_ref.text[1:] != '1b2eecde68d99171f70613f14cf21f49':
            assert os.path.isfile(os.path.join(
                sipdirectory,
                amd_ref.text[1:] + '-PREMIS%3AOBJECT-amd.xml'
                ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        '1b2eecde68d99171f70613f14cf21f49-NISOIMG-amd.xml'
    ))
    # Check that one of the PREMIS techMD files has desired properties
    output_file = os.path.join(
        sipdirectory,
        amd_refs[0].text[1:] + '-PREMIS%3AOBJECT-amd.xml'
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
                      namespaces=NAMESPACES)[0].text == '4.01'

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


@pytest.mark.usefixtures('testmetax', 'mock_metax_access')
# pylint: disable=invalid-name
def test_create_techmd_without_charset(testpath):
    """Test the task with dataset that has files without defined charset

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

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
    reference_file = os.path.join(
        sipdirectory, 'import-object-md-references.xml')
    xml = lxml.etree.parse(reference_file)
    amd_refs = xml.xpath('/mdReferences/mdReference')
    assert len(amd_refs) == 1
    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        amd_refs[0].text[1:] + '-PREMIS%3AOBJECT-amd.xml'
    )
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    # If charset is not defined the siptools.import_objects default value is
    # used. Siptools recognizes ASCII text files as UTF-8 text files.
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'text/html; charset=UTF-8'


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
def test_xml_metadata_file_missing(testpath):
    """Test the workflow task CreateTechnicalMetadata module when XML
    metadata for a file is missing. Behavior not specified yet. Currently
    throws an error.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
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
