"""Tests for :mod:`siptools_research.workflow.create_structmap`."""

import json
import shutil

import pytest
from siptools.scripts.create_mix import create_mix
from siptools.scripts.import_object import import_object
from siptools.utils import read_md_references
from siptools.xml.mets import NAMESPACES
import lxml.etree

import tests.conftest
from siptools_research.workflow.create_structmap import CreateStructMap


def _create_metadata(workspace, files, provenance_ids=None):
    """Create all metadata that is required for structure map creation.

    :param workspace: workflow task workspace directory
    :param files: path to file or directory that contains dataset files
    :returns: ``None``
    """
    sip_creation_path = workspace / 'sip-in-progress'

    # Create dmdsec
    (workspace / 'create-descriptive-metadata.jsonl').write_text(
        '{".": {"md_ids": ["descriptive_metadata_id"]}}'
    )

    # Create digiprov
    if not provenance_ids:
        provenance_ids = []
    (workspace / 'create-provenance-information.jsonl').write_text(
        json.dumps({".": {"md_ids": provenance_ids}})
    )

    # Create tech metadata
    import_object(
        workspace=str(sip_creation_path),
        base_path=str(sip_creation_path),
        skip_wellformed_check=True,
        filepaths=[files],
        event_target='.'
    )
    shutil.move(
        sip_creation_path / "premis-event-md-references.jsonl",
        workspace / "create-technical-metadata.jsonl"
    )


@pytest.mark.parametrize(
    'provenance_ids',
    (
        # No provenance events
        [],
        # One provenance events
        ['procenance_id1'],
        # Multiple provenance events
        ['provenacne_id1', 'provenacne_id2'])
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(workspace, provenance_ids):
    """Test the workflow task CreateStructMap.

    :param workspace: Temporary workspace fixture
    :param provenance_ids: Provenance metadata identifiers
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains many
    # files in directories and subdirectories in sip creation directory
    sip_creation_path = workspace / "sip-in-progress"
    data_directory_path = sip_creation_path / 'data'
    subdirectory_path = data_directory_path / 'subdirectory'
    subdirectory_path.mkdir(parents=True)

    (data_directory_path / "file1").write_text("foo")
    (data_directory_path / "file2").write_text("bar")
    (subdirectory_path / "file3").write_text("baz")

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'data', provenance_ids)

    # Init and run CreateStructMap task
    sip_content_before_run = [
        path.name for path in sip_creation_path.iterdir()
    ]
    task = CreateStructMap(workspace=str(workspace),
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Validate logical filesec XML-file
    filesec_xml = lxml.etree.parse(str(sip_creation_path / 'filesec.xml'))
    files = filesec_xml.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file/mets:FLocat/'
        '@xlink:href',
        namespaces=NAMESPACES
    )
    assert len(files) == 3
    assert set(files) == {'file://data/file1',
                              'file://data/file2',
                              'file://data/subdirectory/file3'}

    # Validate directory structure in structmap XML-file.
    structmap_xml = lxml.etree.parse(str(sip_creation_path / 'structmap.xml'))
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
            str(workspace),
            'create-descriptive-metadata.jsonl'
        )['.']['md_ids'][0]
    assert descriptive_metadata_creation_event_id in structmap_xml.xpath(
        "/mets:mets/mets:structMap/mets:div/@ADMID",
        namespaces=NAMESPACES
    )[0]

    # Structure map should be linked to all provenance events
    for provenance_id in provenance_ids:
        assert provenance_id in structmap_xml.xpath(
            "/mets:mets/mets:structMap/mets:div/@ADMID",
            namespaces=NAMESPACES
        )[0]

    # Filesec.xml, structmap.xml,
    # compile-structmap-agents-AGENTS-amd.json,
    # premis-event-md-references.jsonl, premis event and premis agent
    # should be created into SIP directory.
    files = {path.name for path in sip_creation_path.iterdir()}
    assert len(files) - len(set(sip_content_before_run)) == 6
    assert {'filesec.xml',
            'structmap.xml',
            'compile-structmap-agents-AGENTS-amd.json',
            'premis-event-md-references.jsonl'}.issubset(files)


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_without_directories(workspace):
    """Test creating structmap for dataset without directories.

    :param workspace: Temporary workspace directory fixture
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains only
    # one file
    sip_creation_path = workspace / "sip-in-progress"
    sip_creation_path.mkdir(parents=True)

    (sip_creation_path / "file1").write_text("foo")

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'file1')

    # Init and run CreateStructMap task
    task = CreateStructMap(workspace=str(workspace),
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    task.run()
    assert task.complete()

    # Check structmap file
    xml = lxml.etree.parse(str(sip_creation_path / 'structmap.xml'))
    assert xml.xpath("/mets:mets/mets:structMap/mets:div/@TYPE",
                     namespaces=NAMESPACES)[0] == 'directory'
    assert len(xml.xpath('/mets:mets/mets:structMap/mets:div/mets:fptr'
                         '/@FILEID',
                         namespaces=NAMESPACES)) == 1


@pytest.mark.usefixtures('testmongoclient')
def test_filesec_othermd(workspace):
    """Test CreateStructMap task for dataset with othermd metadata.

    :param workspace: Temporary packaging directory fixture
    :returns: ``None``
    """
    # Create clean workspace directory for dataset that contains only
    # one image file
    sip_creation_path = workspace / "sip-in-progress"
    sip_creation_path.mkdir(parents=True)
    shutil.copy(
        'tests/data/sample_files/image_png.png',
        sip_creation_path / 'file1.png'
    )

    # Create required metadata in workspace directory
    _create_metadata(workspace, 'file1.png')

    # Create mix metadata
    create_mix('file1.png',
               workspace=str(sip_creation_path),
               base_path=str(sip_creation_path))

    # Init and run CreateStructMap task
    task = CreateStructMap(workspace=str(workspace),
                           dataset_id='create_structmap_test_dataset',
                           config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    task.run()
    assert task.complete()

    # Filesec should contain one file which is linked to MIX metadata
    xml = lxml.etree.parse(str(sip_creation_path / 'filesec.xml'))
    files = xml.xpath('/mets:mets/mets:fileSec/mets:fileGrp/mets:file',
                      namespaces=NAMESPACES)
    assert len(files) == 1
    assert '_6169c6604c7da29ff5f0e21cc73478cd' in files[0].attrib['ADMID']
