"""Tests for :mod:`siptools_research.workflow.create_logical_structmap`."""  # noqa: W505,E501

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


@pytest.mark.parametrize(
    ("events", "admids", "provenance_is_from_qvain"),
    [
        (
            [], [], False
        ),
        (
            ['creation'], ['_2005686d72f58850765d1c8147d05cb2'], False
        ),
        (
            ['creation', 'foobar'],
            ['_2005686d72f58850765d1c8147d05cb2',
             '_f8384d1f8b9cbcafcba9370d1b506a26'],
            False
        ),
        (
            ['creation'], ['_2005686d72f58850765d1c8147d05cb2'], True
        ),
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(
    workspace, requests_mock, events, admids, provenance_is_from_qvain
):
    """Test the workflow task CreateLogicalStructMap.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param events: List of provenance events in dataset
    :param admids: List of identifiers of administrative metadata to
                   which the logical structuremap should refer.
    :returns: ``None``
    """
    # Create a dataset
    # Dataset contains two files
    files = [copy.deepcopy(tests.metax_data.files.BASE_FILE),
             copy.deepcopy(tests.metax_data.files.BASE_FILE)]
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
        else:
            provenance = copy.deepcopy(
                tests.metax_data.datasets.BASE_PROVENANCE
            )
            provenance["preservation_event"]["pref_label"]["en"] = event
        dataset["research_dataset"]["provenance"].append(provenance)
    if not dataset["research_dataset"]["provenance"]:
        del dataset["research_dataset"]["provenance"]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Create workspace that already contains dataset files
    sip_directory = workspace / "preservation" / "sip-in-progress"
    file_directory = workspace / 'dataset_files' / 'files'
    file_directory.mkdir(parents=True)

    (file_directory / "file1").write_text("foo")
    (file_directory / "file2").write_text("bar")

    # Create required metadata to workspace:
    # * digital provenance metadata
    # * descriptive metadata
    # * technical metadata
    # * physical structure map
    for event in events:
        premis_event(
            workspace=str(sip_directory),
            event_type=event,
            event_datetime='2014-12-31T08:19:58Z',
            event_detail='foo',
            event_outcome='success',
            event_outcome_detail='bar'
        )
    if events:
        shutil.copy(
            sip_directory / 'premis-event-md-references.jsonl',
            workspace / 'preservation' / 'create-provenance-information.jsonl'
        )
    import_description(
        dmdsec_location='tests/data/datacite_sample.xml',
        workspace=str(sip_directory)
    )
    import_object(
        workspace=str(sip_directory),
        base_path=str(workspace),
        skip_wellformed_check=True,
        filepaths=[str(file_directory)]
    )
    compile_structmap(
        workspace=str(sip_directory),
        structmap_type='Fairdata-physical'
    )

    # Init and run CreateLogicalStructMap task
    sip_prerun_files = {path.name for path in sip_directory.iterdir()}
    task = CreateLogicalStructMap(dataset_id=workspace.name,
                                  config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    validate_logical_structmap_file(
        str(sip_directory / 'logical_structmap.xml'), admids
    )

    sip_postrun_files = {path.name for path in sip_directory.iterdir()}

    # Nothing else should be created SIP directory
    assert sip_postrun_files \
        == sip_prerun_files | {'logical_structmap.xml'}


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


def validate_logical_structmap_file(logical_structmap_file, admids):
    """Validate logical structuremap XML-file.

    Checks that XML-file has the correct elements. Raises exception if
    XML is not valid.

    :param logical_structmap_file: XML file to be validated
    :param admids: List of identifiers of administrative metadata to
                   which the logical structuremap should refer.
    :returns: ``None``
    """
    tree = lxml.etree.parse(logical_structmap_file)
    assert tree.xpath('/mets:mets/mets:structMap',
                      namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "Fairdata-logical"
    assert tree.xpath('/mets:mets/mets:structMap/mets:div',
                      namespaces=NAMESPACES)[0].attrib['TYPE'] \
        == "logical"
    for admid in admids:
        assert admid in \
            tree.xpath('/mets:mets/mets:structMap/mets:div',
                       namespaces=NAMESPACES)[0].attrib['ADMID'].split()

    directories = tree.xpath(
        '/mets:mets/mets:structMap/mets:div/mets:div/@TYPE',
        namespaces=NAMESPACES
    )
    assert len(directories) == 1
    assert 'pid:urn:identifier' in directories

    assert len(tree.xpath('/mets:mets/mets:structMap/mets:div/mets:div'
                          '[@TYPE="pid:urn:identifier"]/mets:fptr/@FILEID',
                          namespaces=NAMESPACES)) == 2
