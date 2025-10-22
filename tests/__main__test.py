"""Tests for :mod:`siptools_research.__main__` module."""
import copy

import pytest
from metax_access.template_data import DATASET

import tests.utils
from siptools_research.dataset import Dataset


def test_main_status_match(config, cli_runner, requests_mock):
    """Test that dataset workflow information is printed correctly.

    :param config: Configuration file
    :param cli_runner: Click CLI runner
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock)

    # Add a single workflow document and a couple of workflow tasks to
    # the db
    dataset = Dataset("test_dataset_id", config=config)
    dataset.preserve()
    dataset.log_task(
        "CreateWorkspace",
        "success",
        "Workspace directory created"
    )
    dataset.log_task(
        "ValidateMetadata",
        "success",
        "Metax metadata in valid"
    )
    dataset.log_task(
        "CreateProvenanceInformation",
        "failure",
        "Fail message"
    )

    # Run siptools-research status 1
    result = cli_runner([
        "--config", config,
        "dataset", "status", "test_dataset_id"
        ]
    )

    # The output is a pretty-printed Python dict: evaluate it to inspect
    # it
    status_data = eval(result.output)

    assert status_data["_id"] == "test_dataset_id"
    assert status_data["target"] == "preservation"

    create_workspace = status_data["workflow_tasks"]["CreateWorkspace"]
    assert create_workspace["messages"] == "Workspace directory created"
    assert create_workspace["result"] == "success"

    validate_metadata = status_data["workflow_tasks"]["ValidateMetadata"]
    assert validate_metadata["messages"] == "Metax metadata in valid"
    assert validate_metadata["result"] == "success"

    create_provenance = \
        status_data["workflow_tasks"]["CreateProvenanceInformation"]
    assert create_provenance["messages"] == "Fail message"
    assert create_provenance["result"] == "failure"


def test_main_list(config, cli_runner, requests_mock):
    """Test that list of datasets is printed correctly.

    :param config: Configuration file
    :param cli_runner: Click CLI runner
    :param requests_mock: HTTP request mocker
    """
    for i in range(10):
        # Mock metax
        dataset_metadata = copy.deepcopy(DATASET)
        dataset_metadata["id"] = f"aineisto_{i}"
        tests.utils.add_metax_dataset(requests_mock, dataset=dataset_metadata)

        # Add workflow
        dataset = Dataset(f"aineisto_{i}", config=config)
        dataset.preserve()

    result = cli_runner([
        "--config", config, "dataset", "list"
    ])

    for i in range(10):
        assert f"aineisto_{i}" in result.output


@pytest.mark.parametrize(
    "command",
    [
        ["dataset", "status", "1"],
        ["dataset", "enable", "1"],
        ["dataset", "disable", "1"]
    ]
)
def test_main_status_no_match(config, cli_runner, command):
    """Test that missing dataset prints correct error for all dataset commands

    :param config: Configuration file
    :param cli_runner: Click CLI runner
    :param command: List of CLI arguments
    """
    result = cli_runner(["--config", config, *command])

    assert "Dataset not found" in result.output


def test_main_disabled(config, cli_runner, requests_mock):
    """Test that the disable and enable commands set the correct dataset as
    disabled and enabled respectively.

    :param config: Configuration file
    :param cli_runner: Click CLI runner
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock)

    # Add a single workflow document to the db
    Dataset("test_dataset_id", config=config).preserve()

    # Disable the dataset using CLI
    result = cli_runner([
        "--config", config,
        "dataset", "disable", "test_dataset_id"
    ])

    assert not Dataset("test_dataset_id", config=config).enabled
    assert "Workflow of dataset test_dataset_id disable" in result.output

    # Enable the dataset using CLI
    result = cli_runner([
        "--config", config,
        "dataset", "enable", "test_dataset_id"
    ])

    assert Dataset("test_dataset_id", config=config).enabled
    assert "Workflow of dataset test_dataset_id enabled" in result.output
