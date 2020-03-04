"""Tests for :mod:`siptools_research.__main__` module"""
import sys

import mock
import pytest

import siptools_research.__main__
from siptools_research.utils.database import Database
from tests.conftest import UNIT_TEST_CONFIG_FILE


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_generate(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "generate"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "generate" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools-research', 'generate', '1']):
        siptools_research.__main__.main()

    # The generate_metadata function should be called.
    mock_generate.assert_called_with('1', '/etc/siptools_research.conf')
    mock_validate.assert_not_called()
    mock_preserve.assert_not_called()


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_validate(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "validate"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "validate" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools-research', 'validate', '2']):
        siptools_research.__main__.main()

    # The validate_metadata function should be called.
    mock_validate.assert_called_with('2', '/etc/siptools_research.conf')
    mock_generate.assert_not_called()
    mock_preserve.assert_not_called()


@mock.patch('siptools_research.__main__.preserve_dataset')
@mock.patch('siptools_research.__main__.validate_metadata')
@mock.patch('siptools_research.__main__.generate_metadata')
def test_main_preserve(mock_generate, mock_validate, mock_preserve):
    """Test that correct function is called from main function when "preserve"
    command is used.

    :param mock_generate: Mock object for `generate_metadata` function
    :param mock_validate: Mock object for `validate_metadata` function
    :param mock_preserve: Mock object for `preserve_dataset` function
    :returns: ``None``
    """
    # Run main function with "preserve" as command
    with mock.patch.object(sys, 'argv',
                           ['siptools-research', 'preserve', '3']):
        siptools_research.__main__.main()

    # The preserve_dataset function should be called.
    mock_preserve.assert_called_with('3', '/etc/siptools_research.conf')
    mock_generate.assert_not_called()
    mock_validate.assert_not_called()


@pytest.mark.usefixtures('testmongoclient')
def test_main_workflow_match(capsys, monkeypatch):
    """Test that workflow command returns the correct workflow.

    :returns: ``None``
    """
    # Add a single workflow documents to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

    # Run siptools-research workflow 1
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflow", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    assert '"_id": "aineisto_1"' in out


@pytest.mark.usefixtures('testmongoclient')
def test_main_workflow_no_matches(capsys, monkeypatch):
    """Test that worklow command prints the correct error message.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

    # Run siptools-research worklow 2
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "workflow", "aineisto_2"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    error = 'Could not find document with workflow identifier: aineisto_2'
    assert error in out


@pytest.mark.usefixtures('testmongoclient')
def test_main_workflows_match(capsys, monkeypatch):
    """Test that workflows command returns the correct workflow.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

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


@pytest.mark.usefixtures('testmongoclient')
def test_main_workflows_no_matches(capsys, monkeypatch):
    """Test that worklows command prints correct error message.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

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


@pytest.mark.usefixtures('testmongoclient')
def test_main_status(capsys, monkeypatch):
    """Test that status command prints the correct workflow status.

    :returns: ``None``
    """
    # Add a single workflow document to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

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
    message = "Status: Request received\nWorkflow is incomplete and enabled\n"
    assert out == message


@pytest.mark.usefixtures('testmongoclient')
def test_main_tasks(capsys, monkeypatch):
    """Test that tasks command collects and groups all workflow tasks
    correctly.

    :returns: ``None``
    """
    # Add a single workflow documents and couple workflow_tasks to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")
    database.add_event(
        "aineisto_1",
        "CreateWorkspace",
        "success",
        "Workspace directory created"
    )
    database.add_event(
        "aineisto_1",
        "ValidateMetadata",
        "success",
        "Metax metadata in valid"
    )
    database.add_event(
        "aineisto_1",
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


@pytest.mark.usefixtures('testmongoclient')
def test_main_dataset(capsys, monkeypatch):
    """Test that dataset command prints all matching workflows or a proper
    error message.

    :returns: ``None``
    """
    # Add two workflow documents to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")
    database.add_workflow("aineisto_2", "1")

    # Run siptools-research dataset 1
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "dataset", "1"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    assert out == "aineisto_1\naineisto_2\n"

    # Run siptools-research dataset 2
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "dataset", "2"
        ]
    )
    siptools_research.__main__.main()

    out, _ = capsys.readouterr()
    assert "No workflows found" in out


@pytest.mark.usefixtures('testmongoclient')
def test_main_disabled(capsys, monkeypatch):
    """Test that the disable and enable commands set the correct dataset as
    disabled and enabled respectively.

    :returns: ``None``
    """
    # Add a single workflow documents to the db
    database = Database(UNIT_TEST_CONFIG_FILE)
    database.add_workflow("aineisto_1", "1")

    # Run siptools-research enable 1
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "enable", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    assert not database._collection.find_one({"_id": "aineisto_1"})["disabled"]
    out, _ = capsys.readouterr()
    assert "Workflow aineisto_1 enabled" in out

    # Run siptools-research enable 1
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "disable", "aineisto_1"
        ]
    )
    siptools_research.__main__.main()
    assert database._collection.find_one({"_id": "aineisto_1"})["disabled"]
    out, _ = capsys.readouterr()
    assert "Workflow aineisto_1 disabled" in out


@mock.patch('siptools_research.__main__.clean_file_cache')
def test_main_clean_cache(mocked_clean_cache, monkeypatch):
    """Test that clean_cache function is called when clean-cache subcommand is
    used.
    """
    # Run siptools-research clean-cache
    monkeypatch.setattr(
        sys, "argv", [
            "siptools-research",
            "--config", UNIT_TEST_CONFIG_FILE,
            "clean-cache"
        ]
    )
    siptools_research.__main__.main()

    # Check that clean_cache function was called with configuration file path
    # as parameter
    mocked_clean_cache.assert_called_once_with(
        'tests/data/configuration_files/siptools_research_unit_test.conf'
    )
