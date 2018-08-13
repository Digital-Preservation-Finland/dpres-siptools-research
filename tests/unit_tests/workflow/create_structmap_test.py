"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os
import shutil
import distutils.dir_util
import pytest
import tests.conftest
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools.scripts import import_object
from siptools.scripts import import_description
from siptools.scripts import premis_event
from siptools.xml.mets import NAMESPACES
import lxml.etree


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_structmap_ok(testpath):
    """Test the workflow task CreateStructMap.

    :testpath: Temporary directory fixture
    :returns: None
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

    # Create digiprov
    event_type = 'creation'
    event_datetime = '2014-12-31T08:19:58Z'
    event_detail = 'Description of provenance'
    premis_event.main([
        event_type, event_datetime,
        "--event_detail", event_detail,
        "--event_outcome", 'success',
        "--workspace", sip_creation_path
    ])

    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object.main([test_data_folder, '--workspace', sip_creation_path])

    # Create structmap
    task = CreateStructMap(workspace=workspace,
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    task.run()
    assert task.complete()
    assert os.path.isfile(os.path.join(sip_creation_path, 'filesec.xml'))
    validate_filesec_file(os.path.join(sip_creation_path, 'filesec.xml'))
    assert os.path.isfile(os.path.join(sip_creation_path, 'structmap.xml'))
    validate_structmap_file(os.path.join(sip_creation_path, 'structmap.xml'))
    assert os.path.isfile(os.path.join(sip_creation_path,
                                       'logical_structmap.xml'))
    validate_logical_structmap_file(os.path.join(sip_creation_path,
                                                 'logical_structmap.xml'))

    with open(os.path.join(sip_creation_path,
                           'logical_structmap.xml'))\
            as open_file:
        file_content = open_file.read()
        assert 'Fairdata-logical' in file_content


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
# pylint: disable=invalid-name
def test_create_structmap_without_directories(testpath):
    """Test creating structmap for dataset that does not have directories.

    :testpath: Temporary directory fixture
    :returns: None
    """
    # Copy workspace directory
    distutils.dir_util.copy_tree('tests/data/workspaces/create_structmap_2',
                                 testpath)

    # Init task
    task = CreateStructMap(workspace=testpath,
                           dataset_id='create_structmap_test_dataset_no_directories',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Run task
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_filesec_othermd(testpath):
    """Test CreateStructMap task with dataset that has some othermd metadata
    for files.

    :testpath: Temporary directory fixture
    :returns: None
    """
    # Copy workspace directory
    distutils.dir_util.copy_tree('tests/data/workspaces/create_structmap_3',
                                 testpath)

    # Init task
    task = CreateStructMap(workspace=testpath,
                           dataset_id='create_structmap_test_dataset_othermd_present',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # Run task
    task.run()
    assert task.complete()


def validate_logical_structmap_file(logical_structmap_file):
    """Validates logical structuremap XML-file. Checks that XML-file has the
    correct elements. Raises exception if XML is not valid.

    :logical_structmap_file: XML file to be validated
    :returns: None
    """
    tree = lxml.etree.parse(logical_structmap_file)
    root = tree.getroot()
    directories = root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    /@TYPE', namespaces=NAMESPACES)
    assert len(directories) == 5
    assert 'Documentation files' in directories
    assert 'Machine-readable metadata' in directories
    assert 'Access and use rights files' in directories
    assert 'Software files' in directories
    assert 'Publication files' in directories
    assert len(root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    [@TYPE="Documentation files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 5
    assert len(root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    [@TYPE="Machine-readable metadata"]/mets:fptr[@FILEID]',
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    [@TYPE="Access and use rights files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    [@TYPE="Software files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    [@TYPE="Publication files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1


def validate_filesec_file(filesec_file):
    """Validates logical filesec XML-file. Checks that XML-file has the
    correct elements. Raises exception if XML is not valid.

    :filesec_file: XML file to be validated
    :returns: None
    """
    tree = lxml.etree.parse(filesec_file)
    root = tree.getroot()
    files = root.xpath('//mets:mets/mets:fileSec/mets:fileGrp/mets:file/\
    mets:FLocat/@xlink:href', namespaces=NAMESPACES)
    assert len(files) == 9
    assert 'file://tests/data/structured/Documentation+files/' \
           'Configuration+files/properties.txt' in files
    assert 'file://tests/data/structured/Documentation+files/' \
           'Other+files/this.txt' in files
    assert 'file://tests/data/structured/Documentation+files/' \
           'readme.txt' in files
    assert 'file://tests/data/structured/Documentation+files/Notebook/' \
           'notes.txt' in files
    assert 'file://tests/data/structured/Documentation+files/Method+files/' \
           'method_putkisto.txt' in files
    assert 'file://tests/data/structured/Machine-readable+metadata/' \
           'metadata.txt' in files
    assert 'file://tests/data/structured/Access+and+use+rights+files/' \
           'access_file.txt' in files
    assert 'file://tests/data/structured/Software+files/koodi.java' in files
    assert 'file://tests/data/structured/Publication+files/' \
           'publication.txt' in files


def validate_structmap_file(structmap_file):
    """Validates logical structuremap XML-file. Checks that XML-file has the
    correct elements. Raises exception if XML is not valid.

    :filesec_file: XML file to be validated
    :returns: None
    """
    tree = lxml.etree.parse(structmap_file)
    root = tree.getroot()
    assert root.xpath("//mets:mets/mets:structMap/mets:div/@TYPE",
                      namespaces=NAMESPACES)[0] == 'directory'
    assert root.xpath("//mets:mets/mets:structMap/mets:div/mets:div/@TYPE",
                      namespaces=NAMESPACES)[0] == 'tests'
    assert root.xpath("//mets:mets/mets:structMap/mets:div/mets:div/mets:div\
    /@TYPE", namespaces=NAMESPACES)[0] == 'data'
    assert root.xpath("//mets:mets/mets:structMap/mets:div/mets:div/mets:div\
    /mets:div/@TYPE", namespaces=NAMESPACES)[0] == 'structured'
    directories = root.xpath("//mets:mets/mets:structMap/mets:div/mets:div\
    /mets:div/mets:div/mets:div/@TYPE", namespaces=NAMESPACES)
    assert 'Documentation files' in directories
    assert 'Machine-readable metadata' in directories
    assert 'Access and use rights files' in directories
    assert 'Software files' in directories
    assert 'Publication files' in directories
    sub_dirs = root.xpath('//mets:mets/mets:structMap/mets:div/mets:div\
    /mets:div/mets:div/mets:div[@TYPE="Documentation files"]/mets:div/@TYPE',
                          namespaces=NAMESPACES)
    assert 'Configuration files' in sub_dirs
    assert 'Other files' in sub_dirs
    assert 'Notebook' in sub_dirs
    assert 'Method files' in sub_dirs
