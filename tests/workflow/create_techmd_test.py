"""Test the :mod:`siptools_research.create_sip.create_techmd` module"""

import os
import shutil
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools_research.workflow.create_techmd import main
import lxml.etree as ET
from siptools.xml.mets import NAMESPACES

def test_create_techmd_ok(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateTechnicalMetadata module.
    """
    # Create workspace with "logs" directory in temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))

    # Copy sample files to workspace directory "sip-in-progress" directory
    shutil.copytree('tests/data/files/',
                    os.path.join(workspace, 'sip-in-progress'))

    # Init task
    task = CreateTechnicalMetadata(workspace=workspace,
                                   dataset_id="3",
                                   config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that XML files are created in workspace
    assert os.path.isfile(os.path.join(
        workspace,
        'project_x_FROZEN%2Fsome%2Fpath%2Ffile_name_5-mets-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'project_x_FROZEN%2Fsome%2Fpath%2Ffile_name_6-mets-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'file_name_5-techmd.xml'
    ))
    assert os.path.isfile(os.path.join(
        workspace,
        'file_name_6-techmd.xml'
    ))

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-create-technical-metadata.log')) as open_file:
        assert open_file.read().startswith(
            "Wrote METS technical metadata to file"
        )


def test_import_object_ok(testpath, testmetax):

    # Copy sample files to 'sip-in-progress' directory in workspace
    testbasepath = os.path.join(testpath, 'sip-in-progress')
    os.makedirs(testbasepath)
    metax_filepath = os.path.join(testbasepath, 'project_x_FROZEN/some/path')
    os.makedirs(os.path.join(metax_filepath))
    shutil.copy('tests/data/file_name_5',
                os.path.join(testbasepath, metax_filepath, 'file_name_5'))
    shutil.copy('tests/data/file_name_6',
                os.path.join(testbasepath, metax_filepath, 'file_name_6'))

    # Run import_objects script for a sample dataset
    main('3', testpath, 'tests/data/siptools_research.conf')

    # Check that output file is created, and it has desired properties
    output_file = os.path.join(testpath, 'file_name_5-techmd.xml')
    tree = ET.parse(output_file)
    root = tree.getroot()
    assert len(root.findall('{http://www.loc.gov/METS/}amdSec')) == 1
    assert len(root.xpath("//premis:object", namespaces=NAMESPACES)) == 1
    assert root.xpath("//premis:object/@*", namespaces=NAMESPACES)[0] == 'premis:file'
    assert root.xpath("//premis:formatName", namespaces=NAMESPACES)[0].text == 'html/text'
    assert root.xpath("//premis:formatVersion", namespaces=NAMESPACES)[0].text == '1.0'
