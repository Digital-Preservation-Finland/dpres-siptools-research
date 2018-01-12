"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools_research.workflow.create_techmd import import_objects
import lxml.etree
from siptools.xml.mets import NAMESPACES


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
        'ADDML-project_x%2Fsome%2Fpath%2Ffile.csv-othermd.xml'
    ))
    assert os.path.isfile(os.path.join(
        sipdirectory,
        'project_x%2Fsome%2Fpath%2Ffile.csv-techmd.xml'
    ))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-technical-metadata.log')) as open_file:
        assert open_file.read().startswith(
            "Wrote METS technical metadata to file"
        )


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
