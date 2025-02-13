"""Tests for :mod:`siptools_research.workflow.validate_metadata`."""

import copy

import pytest

import tests.metax_data.datasetsV3
import tests.metax_data.filesV3
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata
from tests.utils import add_metax_dataset


@pytest.mark.usefixtures("testmongoclient")
def test_validatemetadata(config, workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has valid metadata.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    """
    # Mock Metax. Create a valid dataset that contains two text
    # files.
    file1 = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    file1["id"] = "identifier1"
    file1["pathname"] = "/path1"
    file2 = copy.deepcopy(tests.metax_data.filesV3.TXT_FILE)
    file2["id"] = "identifier2"
    file2["pathname"] = "/"
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    add_metax_dataset(
        requests_mock=requests_mock,
        dataset=dataset,
        files=[file1, file2]
    )

    # Init task
    task = ValidateMetadata(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task
    task.run()

    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_invalid_metadata(config, workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has invalid metadata that does
    not pass schema validation.

    :param config: Configuration file
    :param workspace: Temporary workspace directory fixture
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax. Remove "contract" from dataset metadata to create an
    # invalid dataset.
    dataset = copy.deepcopy(tests.metax_data.datasetsV3.BASE_DATASET)
    dataset["id"] = workspace.name
    dataset["preservation"]["contract"] = None
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)

    # Init task
    task = ValidateMetadata(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidDatasetMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "None is not of type 'string'" in str(exc.value)
    assert not task.complete()
