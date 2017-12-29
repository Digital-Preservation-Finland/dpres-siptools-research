"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os
import shutil
import distutils.dir_util
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools.scripts import import_object
from siptools.scripts import import_description

def test_create_structmap_ok(testpath, testmongoclient, testmetax):
    """Test the workflow task CreateStructMap.
    """
    workspace = testpath
    sip_creation_path = os.path.join(workspace, "sip-in-progress")

    # Clean workspace and create "logs" directory in temporary directory
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Copy sample datacite.xml to workspace directory
    dmdpath = os.path.join(workspace, 'datacite.xml')
    shutil.copy('tests/data/datacite_sample.xml', dmdpath)

    # Create dmdsec
    import_description.main([dmdpath, '--workspace', sip_creation_path])

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main([test_data_folder, '--workspace', sip_creation_path])

    # Create structmap
    task = CreateStructMap(workspace=workspace,
                           dataset_id='create_structmap_test_dataset_1',
                           config='tests/data/siptools_research.conf')

    task.run()
    assert task.complete()
    assert os.path.isfile(os.path.join(sip_creation_path, 'filesec.xml'))
    assert os.path.isfile(os.path.join(sip_creation_path, 'structmap.xml'))

    with open(os.path.join(sip_creation_path,
                           'structmap.xml'))\
             as open_file:
        file_content = open_file.read()
        assert 'Fairdata-logical' in file_content

def test_create_structmap_without_directories(testpath, testmongoclient,
                                              testmetax):
    """Test creating structmap for dataset that does not have directories.
    """
    # Copy workspace directory
    distutils.dir_util.copy_tree('tests/data/workspaces/create_structmap_2',
                                 testpath)

    # Init task
    task = CreateStructMap(workspace=testpath,
                           dataset_id='create_structmap_test_dataset_2',
                           config='tests/data/siptools_research.conf')

    # Run task
    task.run()
    assert task.complete()
