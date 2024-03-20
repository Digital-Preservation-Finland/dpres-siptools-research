"""Tests for `create_logical_struct_map` method of CreateMets task."""  # noqa: W505,E501

import copy

import pytest
import lxml.etree

from metax_access import Metax

from siptools.xml.mets import NAMESPACES

import tests.utils
from siptools_research.workflow.create_mets import (
    CreateMets, find_dir_use_category, get_dirpath_dict
)


@pytest.mark.parametrize(
    ("events", "provenance_is_from_qvain"),
    [
        (
            [], False
        ),
        (
            ['creation'], False
        ),
        (
            ['creation', 'foobar'], False
        ),
        (
            ['creation'], True
        ),
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_logical_structmap_ok(
    workspace, requests_mock, events, provenance_is_from_qvain
):
    """Test creating logical structure map.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param events: List of provenance events in dataset
    :param provenance_is_from_qvain: Choose type provenance event
    """
    provenance_description = "This is a string"
    # Create a dataset
    # Dataset contains two files
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE),
             copy.deepcopy(tests.metax_data.files.TXT_FILE)]
    files[0]['file_path'] = 'files/file1'
    files[1]['file_path'] = 'files/file2'
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset['identifier'] = workspace.name
    # Dataset contans zero or more events
    dataset["research_dataset"]["provenance"] = []
    for event in events:
        # Provenances made in Qvain have 'lifecycle_event' instead of
        # 'provenance_event'
        if provenance_is_from_qvain:
            provenance = copy.deepcopy(
                tests.metax_data.datasets.QVAIN_PROVENANCE
            )
            provenance["lifecycle_event"]["pref_label"]["en"] = event
            provenance["title"]["en"] = provenance_description
        else:
            provenance = copy.deepcopy(
                tests.metax_data.datasets.BASE_PROVENANCE
            )
            provenance["preservation_event"]["pref_label"]["en"] = event
            provenance["description"]["en"] = provenance_description
        dataset["research_dataset"]["provenance"].append(provenance)
    if not dataset["research_dataset"]["provenance"]:
        del dataset["research_dataset"]["provenance"]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Create workspace that already contains dataset files
    file_directory = workspace / 'metadata_generation/dataset_files/files'
    file_directory.mkdir(parents=True)
    (file_directory / "file1").write_text("foo")
    (file_directory / "file2").write_text("bar")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Validate logical Fairdata-logical structure map
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    structmap = mets.xpath(
        '/mets:mets/mets:structMap[@TYPE="Fairdata-logical"]',
        namespaces=NAMESPACES
    )[0]
    assert structmap.xpath('mets:div',
                           namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "logical"

    # Find the digiprovMD elements that were created for provenance
    # events
    provenance_digiprovmd_elements = mets.xpath(
        f"//*[premis:eventDetail='{provenance_description}']"
        "/ancestor::mets:digiprovMD",
        namespaces=NAMESPACES
    )

    # There should be one digiprovMD element per provenance event
    assert len(provenance_digiprovmd_elements) == len(events)

    # Each provenance event should be linked to logical structMap
    for element in provenance_digiprovmd_elements:
        assert element.attrib['ID'] in structmap.xpath(
            'mets:div',
            namespaces=NAMESPACES
        )[0].attrib['ADMID'].split()

    # There should be one div in structMap
    directories = structmap.xpath(
        'mets:div/mets:div',
        namespaces=NAMESPACES
    )
    assert len(directories) == 1
    assert directories[0].attrib['TYPE'] == 'dummy-use-category'

    # The div should contain two files
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:fptr',
        namespaces=NAMESPACES
    )) == 2


def test_get_dirpath_dict(requests_mock):
    """Test that get_dirpath_dict returns the correct dictionary.

    The dictionary maps dirpath to use_category.

    :param requests_mock: Mocker object
    """
    requests_mock.get(
        "https://metaksi/rest/v2/directories/1",
        json={
            "identifier": "1",
            "directory_path": "/"
        }
    )
    requests_mock.get(
        "https://metaksi/rest/v2/directories/2",
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
