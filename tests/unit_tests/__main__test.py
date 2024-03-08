"""Tests for :mod:`siptools_research.__main__` module."""
import sys

import pytest

import siptools_research.__main__
from siptools_research.dataset import Dataset
from tests.conftest import UNIT_TEST_CONFIG_FILE


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_workflow_match(capsys, monkeypatch):
    """Test that workflow command returns the correct workflow.

    :returns: ``None``
    """
    # Start preservation workflow for a dataset
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Search for the dataset
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflow", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    assert out == ('Dataset identifier: aineisto_1\n'
                   'Target: preservation\n')


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_workflow_no_matches(capsys, monkeypatch):
    """Test that worklow command prints the correct error message.

    :returns: ``None``
    """
    # Start preservation workflow for a dataset
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Search for a dataset that does not yet have a preservation
    # workflow
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflow", "aineisto_2"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    error = 'Could not find dataset with identifier: aineisto_2'
    assert error in out


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_workflows_match(capsys, monkeypatch):
    """Test that workflows command returns the correct workflow.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Run siptools-research workflows --enabled
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflows", "--enabled"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    assert out == "aineisto_1\n"


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_workflows_no_matches(capsys, monkeypatch):
    """Test that worklows command prints correct error message.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Run siptools-research worklow 2
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflows", "--disabled"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    error = 'Could not find any workflows'
    assert error in out


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_status(capsys, monkeypatch):
    """Test that status command prints the correct workflow status.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Run siptools-research status 1
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "status", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    out, _ = capsys.readouterr()
    message = "Workflow is enabled\n"
    assert out == message


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_tasks(capsys, monkeypatch):
    """Test that tasks command collects and groups all workflow tasks
    correctly.

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
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "tasks", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    out, _ = capsys.readouterr()
    assert "CreateWorkspace\nValidateMetadata\n" in out
    assert "CreateProvenanceInformation" in out
    assert '"messages": "Fail message"' in out


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_main_disabled(capsys, monkeypatch):
    """Test that the disable and enable commands set the correct dataset as
    disabled and enabled respectively.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).preserve()

    # Disable the dataset using CLI
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "disable", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    assert not Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).enabled
    out, _ = capsys.readouterr()
    assert "Workflow of dataset aineisto_1 disable" in out

    # Enable the dataset using CLI
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "enable", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    assert Dataset("aineisto_1", config=UNIT_TEST_CONFIG_FILE).enabled
    out, _ = capsys.readouterr()
    assert "Workflow of dataset aineisto_1 enabled" in out
