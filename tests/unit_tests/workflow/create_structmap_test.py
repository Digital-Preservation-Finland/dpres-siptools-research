"""Tests for `create_structmap` method of CreateMets task."""

import copy
import shutil

import pytest
from siptools.xml.mets import NAMESPACES
import lxml.etree

import tests.conftest
import tests.utils
from siptools_research.workflow.create_mets import CreateMets


@pytest.mark.parametrize(
    'provenance_descriptions',
    (
        # No provenance events
        [],
        # One provenance events
        ['provenance1'],
        # Multiple provenance events
        ['provenance1', 'provenance2'])
)
@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_ok(workspace, requests_mock,
                             provenance_descriptions):
    """Test physical structure map creation.

    Creates structMap and fileSec for a dataset that contains some
    files in a directory structure. Checks that structMap and fileSec
    contain expected elements. Checks that structMap is linked to
    provenance event metadata.

    :param workspace: Temporary workspace fixture
    :param requests_mock: HTTP request mocker
    :param provenance_descriptions: Descriptions of provenance events
    """
    # Create dataset that contains three text files
    files = []
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE) for i in range(3)]
    files[0]["file_path"] = "/file1"
    files[1]["file_path"] = "/file2"
    files[2]["file_path"] = "/subdirectory/file3"
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    # Add provenance events to dataset
    if provenance_descriptions:
        dataset["research_dataset"]["provenance"] = []
        for provenance_description in provenance_descriptions:
            provenance \
                = copy.deepcopy(tests.metax_data.datasets.BASE_PROVENANCE)
            provenance["description"]["en"] = provenance_description
            dataset["research_dataset"]["provenance"].append(provenance)
    tests.utils.add_metax_dataset(requests_mock=requests_mock,
                                  dataset=dataset,
                                  files=files)

    # Create files in workspace
    dataset_files = workspace / "metadata_generation/dataset_files"
    subdirectory = dataset_files / "subdirectory"
    subdirectory.mkdir(parents=True)
    (dataset_files / "file1").write_text("foo")
    (dataset_files / "file2").write_text("bar")
    (subdirectory / "file3").write_text("baz")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()
    assert task.complete()

    # Read created METS document
    # NOTE: lxml<4.8 requires path as string. Newer versions support
    # Path objects!
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))

    # fileSec should contain three files
    files = mets.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file/mets:FLocat/'
        '@xlink:href',
        namespaces=NAMESPACES
    )
    assert len(files) == 3
    assert set(files) == {'file://dataset_files/file1',
                          'file://dataset_files/file2',
                          'file://dataset_files/subdirectory/file3'}

    # Validate the "Fairdata-physical" structMap in METS.
    structmap = mets.xpath("//mets:structMap[@TYPE='Fairdata-physical']",
                           namespaces=NAMESPACES)[0]
    assert structmap.xpath(
        "mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'directory'
    assert structmap.xpath(
        "mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'dataset_files'
    assert structmap.xpath(
        "mets:div/mets:div/mets:div/@TYPE",
        namespaces=NAMESPACES
    )[0] == 'subdirectory'
    # Two files should be found in data directory
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 2
    # One file should be found in subdirectory of data directory
    assert len(structmap.xpath(
        'mets:div/mets:div/mets:div'
        '/mets:fptr/@FILEID',
        namespaces=NAMESPACES
    )) == 1

    # Premis events should be created for each provenance event found
    # in dataset metadata
    premis_event_descriptions = [
        element.text for element
        in mets.xpath("//premis:eventDetail", namespaces=NAMESPACES)
    ]
    for provenance_description in provenance_descriptions:
        assert provenance_description in premis_event_descriptions

    # Structure map should be linked to all digiprovMD elements
    digiprovmd_ids = mets.xpath("//mets:digiprovMD/@ID", namespaces=NAMESPACES)
    structmap_admids = structmap.xpath("mets:div/@ADMID",
                                       namespaces=NAMESPACES)[0]
    for digiprovmd_id in digiprovmd_ids:
        assert digiprovmd_id in structmap_admids


@pytest.mark.usefixtures('testmongoclient')
def test_create_structmap_without_directories(workspace, requests_mock):
    """Test creating structmap for dataset without directories.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Create a dataset that contains only one file
    files = [copy.deepcopy(tests.metax_data.files.TXT_FILE)]
    files[0]["file_path"] = "/file1"
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock=requests_mock,
                                  dataset=dataset,
                                  files=files)

    # Create the file in "dataset_files"
    dataset_files = workspace / "metadata_generation/dataset_files"
    dataset_files.mkdir(parents=True)
    (dataset_files / "file1").write_text("foo")

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Check the structmap element
    mets = lxml.etree.parse(str(workspace / "preservation/mets.xml"))
    structmap = mets.xpath("//mets:structMap[@TYPE='Fairdata-physical']",
                           namespaces=NAMESPACES)[0]
    assert structmap.xpath("mets:div/@TYPE",
                           namespaces=NAMESPACES)[0] == 'directory'
    assert structmap.xpath("mets:div/mets:div/@TYPE",
                           namespaces=NAMESPACES)[0] == 'dataset_files'
    assert len(structmap.xpath('mets:div/mets:div/mets:fptr/@FILEID',
                               namespaces=NAMESPACES)) == 1


@pytest.mark.usefixtures('testmongoclient')
def test_filesec_othermd(workspace, requests_mock):
    """Test creating structmap for dataset with othermd metadata.

    Creates METS for a dataset that contains image file. MIX metadata
    should be created, and the file element in fileSec should be linked
    to to MIX metadata.

    :param workspace: Temporary packaging directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Create a dataset that contains an image file
    dataset = copy.deepcopy(tests.metax_data.datasets.BASE_DATASET)
    dataset["identifier"] = workspace.name
    files = [copy.deepcopy(tests.metax_data.files.TIFF_FILE)]
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset, files=files)

    # Copy image file to test workspace
    filepath \
        = workspace / "metadata_generation/dataset_files/path/to/file.tiff"
    filepath.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/valid_tiff.tiff", filepath)

    # Init and run task
    task = CreateMets(dataset_id=workspace.name,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    task.run()

    # Filesec should contain one file
    mets = lxml.etree.parse(str(workspace / 'preservation/mets.xml'))
    file_elements = mets.xpath(
        '/mets:mets/mets:fileSec/mets:fileGrp/mets:file',
        namespaces=NAMESPACES
    )
    assert len(file_elements) == 1

    # The file element should contain link to MIX metadata
    mix_techmd_element = mets.xpath(
        "//mets:mdWrap[@MDTYPE='NISOIMG']/parent::mets:techMD",
        namespaces=NAMESPACES
    )[0]
    assert mix_techmd_element.attrib["ID"] in file_elements[0].attrib['ADMID']
