"""Tests for :mod:`siptools_research.workflow.create_structmap`."""

import os
import shutil

import pytest
from siptools.scripts.create_mix import create_mix
from siptools.scripts.import_object import import_object
from siptools.scripts.import_description import import_description
from siptools.utils import read_md_references
from siptools.scripts.premis_event import premis_event
from siptools.xml.mets import NAMESPACES
import lxml.etree

import tests.conftest
from siptools_research.workflow.create_structmap import CreateStructMap


def _create_metadata(workspace, files):
    """Create all metadata that is required for structure map creation.

    :param workspace: workflow task workspace directory
    :param files: path to file or directory that contains dataset files
    :returns: ``None``
    """
    sip_creation_path = os.path.join(workspace, 'sip-in-progress')

    # Create dmdsec
    import_description(dmdsec_location='tests/data/datacite_sample.xml',
                       workspace=sip_creation_path)
    shutil.move(
        os.path.join(sip_creation_path, 'premis-event-md-references.jsonl'),
        os.path.join(workspace,
                     'create-descriptive-metadata.jsonl')
    )

    # Create digiprov
    premis_event(
        event_type='creation',
        event_datetime='2014-12-31T08:19:58Z',
        event_detail='foo',
        event_outcome='success',
        event_outcome_detail="bar",
        workspace=sip_creation_path
    )
    shutil.move(
        os.path.join(sip_creation_path, 'premis-event-md-references.jsonl'),
        os.path.join(workspace,
                     'create-provenance-information.jsonl')
    )

    # Create tech metadata
    import_object(
        workspace=sip_creation_path,
        base_path=sip_creation_path,
        skip_wellformed_check=True,
        filepaths=[files],
        event_target='.'
    )
    shutil.move(
        os.path.join(sip_creation_path, 'premis-event-md-references.jsonl'),
        os.path.join(workspace,
                     'create-technical-metadata.jsonl')
    )


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(testpath):
    """Test the workflow task CreateStructMap.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains many
    # files in directories and subdirectories in sip creation directory
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    sip_creation_path = os.path.join(workspace, "sip-in-progress")
    data_directory_path = os.path.join(sip_creation_path, 'data')
    subdirectory_path = os.path.join(data_directory_path, 'subdirectory')
    os.makedirs(subdirectory_path)
    with open(os.path.join(data_directory_path, 'file1'), 'w') \
            as file_in_directory:
        file_in_directory.write('foo')
    with open(os.path.join(data_directory_path, 'file2'), 'w') \
            as file_in_directory:
        file_in_directory.write('bar')
    with open(os.path.join(subdirectory_path, 'file3'), 'w') \
            as file_in_subdirectory:
        file_in_subdirectory.write('baz')

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'data')

    # Init and run CreateStructMap task
    sip_content_before_run = os.listdir(sip_creation_path)
    task = CreateStructMap(workspace=workspace,
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Validate logical filesec XML-file
    filesec_xml = lxml.etree.parse(os.path.join(sip_creation_path,
                                                'filesec.xml'))
    files = filesec_xml.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file/mets:FLocat/'
        '@xlink:href',
        namespaces=NAMESPACES
    )
    assert len(files) == 3
    assert set(files) == set(['file://data/file1',
                              'file://data/file2',
                              'file://data/subdirectory/file3'])

    # Validate directory structure in structmap XML-file.
    structmap_xml = lxml.etree.parse(os.path.join(sip_creation_path,
                                                  'structmap.xml'))
    assert structmap_xml.xpath(
        "/mets:mets/mets:structMap/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'directory'
    assert structmap_xml.xpath(
        "/mets:mets/mets:structMap/mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'data'
    assert structmap_xml.xpath(
        "/mets:mets/mets:structMap/mets:div/mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'subdirectory'
    # Two files should be found in data directory
    assert len(structmap_xml.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 2
    # One file should be found in subdirectory of data directory
    assert len(structmap_xml.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/mets:div'
        '/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 1

    # Structure map should be linked to descriptive metadata creation
    # event
    descriptive_metadata_creation_event_id \
        = read_md_references(
            workspace,
            'create-descriptive-metadata.jsonl'
        )['.']['md_ids'][0]
    assert descriptive_metadata_creation_event_id in structmap_xml.xpath(
        "/mets:mets/mets:structMap/mets:div/@ADMID",
        namespaces=NAMESPACES
    )[0]

    # Only premis-event-md-references.jsonl, filesec.xml and
    # structmap.xml be created into SIP directory
    assert set(os.listdir(sip_creation_path)) \
        == set(sip_content_before_run + ['filesec.xml',
                                         'structmap.xml',
                                         'premis-event-md-references.jsonl'])


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_without_directories(testpath):
    """Test creating structmap for dataset without directories.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains only
    # one file
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    sip_creation_path = os.path.join(workspace, "sip-in-progress")
    os.makedirs(sip_creation_path)
    with open(os.path.join(sip_creation_path, 'file1'), 'w') \
            as file_in_directory:
        file_in_directory.write('foo')

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'file1')

    # Init and run CreateStructMap task
    task = CreateStructMap(workspace=workspace,
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    task.run()
    assert task.complete()

    # Check structmap file
    xml = lxml.etree.parse(os.path.join(sip_creation_path, 'structmap.xml'))
    assert xml.xpath("/mets:mets/mets:structMap/mets:div/@TYPE",
                     namespaces=NAMESPACES)[0] == 'directory'
    assert len(xml.xpath('/mets:mets/mets:structMap/mets:div/mets:fptr'
                         '/@FILEID',
                         namespaces=NAMESPACES)) == 1


@pytest.mark.usefixtures('testmongoclient')
def test_filesec_othermd(testpath):
    """Test CreateStructMap task for dataset with othermd metadata.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains only
    # one image file
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    sip_creation_path = os.path.join(workspace, "sip-in-progress")
    os.makedirs(sip_creation_path)
    shutil.copy('tests/data/sample_files/image_png.png',
                os.path.join(sip_creation_path, 'file1.png'))

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'file1.png')

    # Create mix metadata
    create_mix('file1.png',
               workspace=sip_creation_path,
               base_path=sip_creation_path)

    # Init and run CreateStructMap task
    task = CreateStructMap(workspace=workspace,
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    task.run()
    assert task.complete()

    # Filesec should contain one file which is linked to MIX metadata
    xml = lxml.etree.parse(os.path.join(sip_creation_path, 'filesec.xml'))
    files = xml.xpath('/mets:mets/mets:fileSec/mets:fileGrp/mets:file',
                      namespaces=NAMESPACES)
    assert len(files) == 1
    assert '_6169c6604c7da29ff5f0e21cc73478cd' in files[0].attrib['ADMID']
