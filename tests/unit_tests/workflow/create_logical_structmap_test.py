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
    CreateLogicalStructMap, find_dir_use_category, get_dirpath_dict
)


@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'mock_metax_access')
def test_create_structmap_ok(testpath, requests_mock):
    """Test the workflow task CreateLogicalStructMap.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    # Mock research_dataset directories
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:1",
        json={
            "identifier": "pid:urn:dir:1",
            "directory_path": "/access"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:2",
        json={
            "identifier": "pid:urn:dir:2",
            "directory_path": "/docs"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:3",
        json={
            "identifier": "pid:urn:dir:3",
            "directory_path": "/metadata"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:4",
        json={
            "identifier": "pid:urn:dir:4",
            "directory_path": "/pubs"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/pid:urn:dir:5",
        json={
            "identifier": "pid:urn:dir:5",
            "directory_path": "/software"
        }
    )

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


def test_get_dirpath_dict(requests_mock):
    """Test that get_dirpath_dict returns the correct dictionary, which maps
    dirpath -> use_category.
    """
    requests_mock.get(
        "https://metaksi/rest/v1/directories/1",
        json={
            "identifier": "1",
            "directory_path": "/"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v1/directories/2",
        json={
            "identifier": "2",
            "directory_path": "/test"
        }
    )

    metax_client = Metax("https://metaksi", "test", "test")
    dataset_metadata = {
        "research_dataset": {
            "directories": [
                {
                    "identifier": "1",
                    "use_category": {"pref_label": {"en": "rootdir"}}
                },
                {
                    "identifier": "2",
                    "use_category": {"pref_label": {"en": "testdir"}}
                }
            ]
        }
    }

    assert get_dirpath_dict(metax_client, dataset_metadata) == {
        "/": {"pref_label": {"en": "rootdir"}},
        "/test": {"pref_label": {"en": "testdir"}}
    }


def test_get_dirpath_dict_no_directories():
    """Test that get_dirpath_dict returns an empty dict when no directories
    are defined in the research_dataset.
    """
    metax_client = Metax("https://metaksi", "test", "test")
    assert not get_dirpath_dict(metax_client, {"research_dataset": {}})


def test_find_dir_use_category():
    """Test that find_dir_use_category returns the correct label"""
    dirpath_dict = {
        "/test1": {"pref_label": {"en": "testdir1"}},
        "/test2": {"pref_label": {"en": "testdir2"}}
    }
    languages = ["en"]

    # Straightforward cases
    assert find_dir_use_category("/test1", dirpath_dict, languages) == "testdir1"
    assert find_dir_use_category("/test2", dirpath_dict, languages) == "testdir2"

    # Closest parent that matches
    assert find_dir_use_category(
        "/test1/test", dirpath_dict, languages
    ) == "testdir1"

    # No matches
    assert not find_dir_use_category("/", dirpath_dict, languages)
    assert not find_dir_use_category("/test3", dirpath_dict, languages)

    # No directories were found in the research_dataset
    assert not find_dir_use_category("/test", {}, languages)

    # Match to root
    assert find_dir_use_category(
        "/",
        {"/": {"pref_label": {"en": "root"}}},
        languages
    ) == "root"


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
