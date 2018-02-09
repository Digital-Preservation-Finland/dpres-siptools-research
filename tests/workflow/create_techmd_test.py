"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools_research.workflow.create_techmd import import_objects
import lxml.etree
from siptools.xml.mets import NAMESPACES
import pytest


def test_create_techmd_ok(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateTechnicalMetadata module.
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
                                   dataset_id="create_techmd_test_dataset_1",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML files are created in workspace
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_6-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile.csv-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'ADDML-project_x%2Fsome%2Fpath%2Ffile.csv-othermd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Fvalid_tiff.tiff-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Fvalid_tiff.tiffmix-othermd.xml'
    ))
    output_file = os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Fvalid_tiff.tiffmix-othermd.xml'
    )
    # pylint: disable=no-member
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//mets:techMD/mix:mix", namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicDigitalObjectInformation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics/mix:PhotometricInterpretation",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:BasicImageInformation/mix:SpecialFormatCharacteristics",
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath("//mix:mix/mix:ImageAssessmentMetadata",
                          namespaces=NAMESPACES)) == 1

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-technical-metadata.log')) as open_file:
        assert "Wrote METS technical metadata to file" in open_file.read()


def test_import_object_ok(testpath, testmetax):
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
    import_objects('create_techmd_test_dataset_1',
                   testpath,
                   'tests/data/siptools_research.conf')

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-techmd.xml'
    )
    # pylint: disable=no-member
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] \
        == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'html/text; charset=UTF-8'
    # TODO: reading format version from Metax is not implemented
    # assert root.xpath("//premis:formatVersion",
    #                   namespaces=NAMESPACES)[0].text == '1.0'


def test_import_object_without_charset(testpath, testmetax):
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
    import_objects('create_techmd_test_dataset_2',
                   testpath,
                   'tests/data/siptools_research.conf')

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile_name_5-techmd.xml'
    )
    # pylint: disable=no-member
    tree = lxml.etree.parse(output_file)
    root = tree.getroot()
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text \
        == 'html/text'


def test_xml_metadata_file_missing(testpath, testmongoclient, testmetax):
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
    shutil.copytree('tests/data/sample_dataset_directories/project_xml_metadata_missing',
                    os.path.join(sipdirectory, 'project_xml_metadata_missing'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="create_techmd_test_dataset_xml_metadata_missing",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as exc:
        task.run()
    assert 'Could not retrieve additional metadata XML for dataset pid:urn:8' \
        in str(exc)
    assert not task.complete()


def test_hash_algorithm_selection_logic(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateTechnicalMetadata's hash algorithm
    selection logic. If algorithm is unknown the length of the checksum
    value is used to determine the corresponding algorithm
    """
    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree(
        'tests/data/sample_dataset_directories/project_hash_algorithms',
        os.path.join(sipdirectory, 'project_hash_algorithms'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="create_techmd_test_dataset_hash_algorithms",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()
    techmd_filename = 'project_hash_algorithms%2Fsome%2Fpath%2Fvalid_tiff.tiff-techmd.xml'
    techmd_file = open(os.path.join(sipdirectory, techmd_filename))
    tree = lxml.etree.parse(techmd_file)
    root = tree.getroot()
    assert root.xpath("//premis:messageDigestAlgorithm",
                      namespaces=NAMESPACES)[0].text == 'MD5'
    assert root.xpath("//premis:messageDigest",
                      namespaces=NAMESPACES)[0].text == '54ebe2f9f6e7e78fe5f523887eb55517'


def test_hash_algorithm_error(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateTechnicalMetadata's hash algorithm
    selection logic. If checksum configuration contains invalid data
    an exception is thrown
    """
    # Create workspace with empty "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sipdirectory = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sipdirectory)

    # Copy sample directory with some files to SIP
    shutil.copytree(
        'tests/data/sample_dataset_directories/project_hash_algorithm_error',
        os.path.join(sipdirectory, 'project_hash_algorithms'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="create_techmd_test_dataset_hash_algorithm_error",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as exc:
        task.run()
    assert 'Invalid checksum data (algorithm: sha2, value: habeebit) for file: pid:urn:10' \
        in str(exc)
    assert not task.complete()
