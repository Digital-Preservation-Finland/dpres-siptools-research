"""Tests for :mod:`siptools_research.workflow.create_logical_structmap`."""  # noqa: W505,E501

import os
import copy
import shutil

import pytest
import lxml.etree

from metax_access import Metax

from siptools.scripts.import_description import import_description
from siptools.scripts.import_object import import_object
from siptools.scripts.compile_structmap import compile_structmap
from siptools.scripts.premis_event import premis_event
from siptools.xml.mets import NAMESPACES

import tests.utils
from siptools_research.workflow.create_logical_structmap import (
    CreateLogicalStructMap, find_dir_use_category, get_dirpath_dict
)


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(testpath, requests_mock):
    """Test the workflow task CreateLogicalStructMap.

    :param testpath: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Create a dataset that contains two files
    files = [copy.deepcopy(tests.metax_data.files.BASE_FILE),
             copy.deepcopy(tests.metax_data.files.BASE_FILE)]
    files[0]['file_path'] = 'files/file1'
    files[1]['file_path'] = 'files/file2'
    tests.utils.add_metax_dataset(requests_mock, files=files)

    # Create workspace that already contains dataset files
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    sip_directory = os.path.join(workspace, "sip-in-progress")
    os.makedirs(sip_directory)
    file_directory = os.path.join(workspace, 'dataset_files', 'files')
    os.makedirs(file_directory)
    with open(os.path.join(file_directory, 'file1'), 'w') as file_:
        file_.write('foo')
    with open(os.path.join(file_directory, 'file2'), 'w') as file_:
        file_.write('bar')

    # Create metadata required metadata to workspace:
    # * digital provenance metadata
    # * descriptive metadata
    # * technical metadata
    # * physical structure map
    premis_event(
        workspace=sip_directory,
        event_type='creation',
        event_datetime='2014-12-31T08:19:58Z',
        event_detail='foo',
        event_outcome='success',
        event_outcome_detail='bar'
    )
    shutil.copy(
        os.path.join(sip_directory, 'premis-event-md-references.jsonl'),
        os.path.join(workspace,
                     'create-provenance-information.jsonl')
    )
    import_description(
        dmdsec_location='tests/data/datacite_sample.xml',
        workspace=sip_directory
    )
    import_object(
        workspace=sip_directory,
        base_path=workspace,
        skip_wellformed_check=True,
        filepaths=[file_directory]
    )
    compile_structmap(
        workspace=sip_directory,
        structmap_type='Fairdata-physical'
    )

    # Init and run CreateLogicalStructMap task
    sip_directory_content_before_run = os.listdir(sip_directory)
    task = CreateLogicalStructMap(workspace=workspace,
                                  dataset_id='dataset_identifier',
                                  config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    validate_logical_structmap_file(os.path.join(sip_directory,
                                                 'logical_structmap.xml'))

    # Nothing else should be created SIP directory
    assert set(os.listdir(sip_directory)) \
        == set(sip_directory_content_before_run + ['logical_structmap.xml'])


def test_get_dirpath_dict(requests_mock):
    """Test that get_dirpath_dict returns the correct dictionary.

    The dictionary maps dirpath to use_category.

    :param requests_mock: Mocker object
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


# pylint: disable=invalid-name
def test_get_dirpath_dict_no_directories():
    """Test get_dirpath_dict function with dataset without directories.

    The function should return an empty dict when no directories are
    defined in the research_dataset.
    """
    metax_client = Metax("https://metaksi", "test", "test")
    assert not get_dirpath_dict(metax_client, {"research_dataset": {}})


def test_find_dir_use_category():
    """Test that find_dir_use_category returns the correct label."""
    dirpath_dict = {
        "/test1": {"pref_label": {"en": "testdir1"}},
        "/test2": {"pref_label": {"en": "testdir2"}}
    }
    languages = ["en"]

    # Straightforward cases
    assert find_dir_use_category("/test1", dirpath_dict, languages) \
        == "testdir1"
    assert find_dir_use_category("/test2", dirpath_dict, languages) \
        == "testdir2"

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
    """Validate logical structuremap XML-file.

    Checks that XML-file has the correct elements. Raises exception if
    XML is not valid.

    :param logical_structmap_file: XML file to be validated
    :returns: ``None``
    """
    tree = lxml.etree.parse(logical_structmap_file)
    assert tree.xpath('/mets:mets/mets:structMap',
                      namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "Fairdata-logical"

    directories = tree.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
        namespaces=NAMESPACES
    )
    assert len(directories) == 1
    assert 'pid:urn:identifier' in directories

    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="pid:urn:identifier"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 2
