"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os
import shutil
from json import dumps
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools.scripts import import_object
from siptools.scripts import import_description

def test_create_structmap_ok(testpath, testmongoclient):
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

    # Create provenance
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Create tech metadata
    test_data_folder = './tests/data/structured'

    import_object.main([test_data_folder, '--workspace', sip_creation_path])

    # logical structmap
    logical_struct = {
        'Access and use rights files': [
            'tests/data/structured/Access and use rights files/access_file.txt'
        ],
        'Documentation files': [
            'tests/data/structured/Documentation files/readme.txt',
            'tests/data/structured/Documentation files/Configuration files/'\
                    'properties.txt',
            'tests/data/structured/Documentation files/Method files/'\
                    'method_putkisto.txt',
            'tests/data/structured/Documentation files/Notebook/notes.txt',
            'tests/data/structured/Documentation files/Other files/this.txt'],
        'Machine-readable metadata': [
            'tests/data/structured/Machine-readable metadata/metadata.txt'
        ],
        'Publication files': [
            'tests/data/structured/Publication files/publication.txt'
        ],
        'Software files': [
            'tests/data/structured/Software files/koodi.java'
        ]
    }
    with open(os.path.join(workspace,
                           'sip-in-progress',
                           'logical_struct'), 'w') as new_file:
        new_file.write(dumps(logical_struct))

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


def test_create_structmap_get_files_ok(testpath, testmongoclient):

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

    # Create provenance
    testfilename = "aineisto"
    testfilepath = os.path.join(workspace, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    logical_struct = {'Source material': ['tests/data/text-file.txt']}
    with open(os.path.join(workspace,
                           'sip-in-progress',
                           'logical_struct'), 'w') as new_file:
        new_file.write(dumps(logical_struct))

    # Create technical metadata
    import_object.main(['./tests/data/text-file.txt',
                        '--workspace',
                        sip_creation_path])

    # Create structmap
    task = CreateStructMap(workspace=workspace, dataset_id='2',
                           config='tests/data/siptools_research.conf')

    task.run()
