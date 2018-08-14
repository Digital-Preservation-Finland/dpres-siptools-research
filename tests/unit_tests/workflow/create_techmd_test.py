"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
import hashlib
from siptools_research.workflow.create_techmd import (CreateTechnicalMetadata,
                                                      import_objects,
                                                      algorithm_name)
import lxml.etree
from siptools.xml.mets import NAMESPACES
import pytest
import tests.conftest


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_techmd_ok(testpath):
    """Test the workflow task CreateTechnicalMetadata module.

    :testpath: Temporary directory fixture
    :returns: None
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
                                   dataset_id="create_techmd_test_dataset",
                                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML files are created in workspace
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-premis-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_6-premis-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile.csv-premis-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'ADDML-project_x%2Fsome%2Fpath%2Ffile.csv-othermd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Fvalid_tiff.tiff-premis-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        '7587176da018ac09fd03bc65975a6678-NISOIMG-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'techmd-references.xml'
    ))
    output_file = os.path.join(
        sipdirectory,
        '7587176da018ac09fd03bc65975a6678-NISOIMG-techmd.xml'
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

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-technical-metadata.log')) as open_file:
        assert "Wrote METS technical metadata to file" in open_file.read()


@pytest.mark.usefixtures('testmetax')
def test_import_object_ok(testpath):
    """Test import object function"""

    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    os.makedirs(os.path.join(testpath, 'logs'))
    sipdirectory = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree('tests/data/sample_dataset_directories/project_x',
                    os.path.join(sipdirectory, 'project_x'))

    # Run import_objects function for a sample dataset
    import_objects('create_techmd_test_dataset',
                   testpath,
                   tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-premis-techmd.xml'
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


@pytest.mark.usefixtures('testmetax')
# pylint: disable=invalid-name
def test_import_object_without_charset(testpath):
    """Test import object function with dataset that has files without defined
    charset"""

    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree('tests/data/sample_dataset_directories/project_x',
                    os.path.join(sipdirectory, 'project_x'))

    # Run import_objects function for a sample dataset
    import_objects('create_techmd_test_dataset_charset_not_defined',
                   testpath,
                   tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-premis-techmd.xml'
    )
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    # If charset is not defined the siptools.import_objects default value is
    # used. For some reason the default value is set to ISO-8859-15.
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'text/html; charset=ISO-8859-15'


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_xml_metadata_file_missing(testpath):
    """Test the workflow task CreateTechnicalMetadata module when XML
    metadata for a file is missing. Behavior not specified yet. Currently
    throws an error
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
    with pytest.raises(Exception) as exc:
        task.run()
    assert 'Could not retrieve additional metadata XML for dataset pid:urn:8' \
        in str(exc)
    assert not task.complete()


def test_algorithm_name():
    """Test ``algorithm_name`` function with valid and invalid inputs."""

    # Valid input
    assert algorithm_name('md5', hashlib.md5('foo').hexdigest()) == 'MD5'
    assert algorithm_name('sha2', hashlib.sha224('foo').hexdigest()) \
        == 'SHA-224'
    assert algorithm_name('sha2', hashlib.sha256('foo').hexdigest()) \
        == 'SHA-256'
    assert algorithm_name('sha2', hashlib.sha384('foo').hexdigest()) \
        == 'SHA-384'
    assert algorithm_name('sha2', hashlib.sha512('foo').hexdigest()) \
        == 'SHA-512'

    # invalid algorithm name
    with pytest.raises(UnboundLocalError):
        algorithm_name('foo', 'bar')

    # invalid value length
    with pytest.raises(KeyError):
        algorithm_name('sha2', 'foobar')
