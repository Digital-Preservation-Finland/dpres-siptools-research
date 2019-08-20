"""Test the :mod:`siptools_research.workflow.create_structmap` module"""

import os

import pytest
import lxml.etree

from metax_access import Metax

from siptools.scripts.import_description import import_description
from siptools.scripts.import_object import import_object
from siptools.scripts.compile_structmap import compile_structmap
from siptools.scripts.premis_event import create_premis_event_file
from siptools.xml.mets import NAMESPACES

import tests.conftest
from siptools_research.workflow.create_logical_structmap import (
    CreateLogicalStructMap, find_dir_use_category
)


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_create_structmap_ok(testpath):
    """Test the workflow task CreateLogicalStructMap.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Create sip directory in workspace
    sip_creation_path = os.path.join(testpath, "sip-in-progress")
    os.makedirs(os.path.join(testpath, 'sip-in-progress'))

    # Create digiprov
    event_type = 'creation'
    event_datetime = '2014-12-31T08:19:58Z'
    event_detail = 'Description of provenance'

    create_premis_event_file(
        sip_creation_path, event_type, event_datetime,
        event_detail, 'success', event_detail
    )

    # Create dmdsec (required to create valid physical structmap)
    import_description(
        'tests/data/datacite_sample.xml',
        workspace=sip_creation_path
    )
    # Create tech metadata
    test_data_folder = './tests/data/structured'
    import_object(
        workspace=sip_creation_path,
        skip_wellformed_check=True,
        filepaths=[test_data_folder]
    )

    # Create physical structmap
    compile_structmap(
        workspace=sip_creation_path,
        structmap_type='Fairdata-physical'
    )

    # Init and run CreateStructMap task
    task = CreateLogicalStructMap(workspace=testpath,
                                  dataset_id='create_structmap_test_dataset',
                                  config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    validate_logical_structmap_file(os.path.join(sip_creation_path,
                                                 'logical_structmap.xml'))


def test_find_dir_use_category(requests_mock):
    """Test that find_dir_use_category returns the label from
    the closest parent directory.
    """
    requests_mock.get(
        "https://metaksi/rest/v1/directories/1",
        json={
            "identifier": "1",
            "parent_directory": {"identifier": "2"}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/2",
        json={
            "identifier": "2",
            "parent_directory": {"identifier": "3"}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/3",
        json={
            "identifier": "3",
            "parent_directory": {"identifier": "4"},
            "use_category": {"pref_label": {"en": "correct_label"}}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/4",
        json={
            "identifier": "4",
            "use_category": {"pref_label": {"en": "wrong_label"}}
        }
    )
    metax_client = Metax("https://metaksi", "test", "test")
    assert find_dir_use_category(metax_client, "1", ["en"]) == "correct_label"


def test_use_category_missing(requests_mock):
    """Test that function find_dir_use_category traverses all the way to the
    root directory and returns None if no directories have defined the
    use_category.
    """
    requests_mock.get(
        "https://metaksi/rest/v1/directories/1",
        json={
            "identifier": "1",
            "parent_directory": {"identifier": "2"}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/2",
        json={
            "identifier": "2",
            "parent_directory": {"identifier": "3"}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/3",
        json={
            "identifier": "3",
            "parent_directory": {"identifier": "4"}
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/4",
        json={"identifier": "4"}
    )
    metax_client = Metax("https://metaksi", "test", "test")
    assert not find_dir_use_category(metax_client, "1", ["en"])
    assert requests_mock.call_count == 4


def validate_logical_structmap_file(logical_structmap_file):
    """Validates logical structuremap XML-file. Checks that XML-file has the
    correct elements. Raises exception if XML is not valid.

    :param logical_structmap_file: XML file to be validated
    :returns: ``None``
    """
    tree = lxml.etree.parse(logical_structmap_file)

    directories = tree.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
        namespaces=NAMESPACES
    )
    assert len(directories) == 5
    assert 'Documentation files' in directories
    assert 'Machine-readable metadata' in directories
    assert 'Access and use rights files' in directories
    assert 'Software files' in directories
    assert 'Publication files' in directories

    assert tree.xpath('/mets:mets/mets:structMap',
                      namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "Fairdata-logical"

    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Documentation files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 5
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Machine-readable metadata"]/mets:fptr'
                          '[@FILEID]',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Access and use rights files"]/mets:fptr'
                          '/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Software files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="Publication files"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 1
