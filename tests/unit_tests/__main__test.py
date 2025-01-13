"""Tests for :mod:`siptools_research.__main__` module."""
import pytest

from siptools_research.dataset import Dataset
from tests.conftest import UNIT_TEST_CONFIG_FILE


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_status_match(cli_runner):
    """Test that dataset workflow information is printed correctly.

    :returns: ``None``
    """
    # Add a single workflow document and a couple of workflow tasks to
    # the db
    dataset = Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE)
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
        "--config", UNIT_TEST_CONFIG_FILE,
        "dataset", "status", "aineisto_1"
        ]
    )

    # The output is a pretty-printed Python dict: evaluate it to inspect
    # it
    status_data = eval(result.output)

    assert status_data["_id"] == "aineisto_1"
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


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_list(cli_runner):
    """Test that list of datasets is printed correctly."""
    for i in range(0, 10):
        dataset = Dataset(f"aineisto_{i}", config=UNIT_TEST_CONFIG_FILE)
        dataset.preserve()

    result = cli_runner([
        "--config", UNIT_TEST_CONFIG_FILE, "dataset", "list"
    ])

    for i in range(0, 10):
        assert f"aineisto_{i}" in result.output


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
@pytest.mark.parametrize(
    "command",
    [
        ["dataset", "status", "1"],
        ["dataset", "enable", "1"],
        ["dataset", "disable", "1"]
    ]
)
def test_main_status_no_match(cli_runner, command):
    """Test that missing dataset prints correct error for all dataset commands

    :returns: ``None``
    """
    result = cli_runner([
        "--config", UNIT_TEST_CONFIG_FILE,
        *command
    ])

    assert "Dataset not found" in result.output


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_disabled(cli_runner):
    """Test that the disable and enable commands set the correct dataset as
    disabled and enabled respectively.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Disable the dataset using CLI
    result = cli_runner([
        "--config", UNIT_TEST_CONFIG_FILE,
        "dataset", "disable", "aineisto_1"
    ])

    assert not Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).enabled
    assert "Workflow of dataset aineisto_1 disable" in result.output

    # Enable the dataset using CLI
    result = cli_runner([
        "--config", UNIT_TEST_CONFIG_FILE,
        "dataset", "enable", "aineisto_1"
    ])

    assert Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).enabled
    assert "Workflow of dataset aineisto_1 enabled" in result.output
