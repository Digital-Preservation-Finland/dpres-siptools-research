"""Tests for :mod:`siptools_research.workflow_init` module."""
import copy
from pathlib import Path

import pytest
from metax_access.template_data import DATASET

import tests.utils
from siptools_research.dataset import Dataset
from siptools_research.models.workflow_entry import WorkflowEntry
from siptools_research.tasks.cleanup import Cleanup
from siptools_research.tasks.generate_metadata import GenerateMetadata
from siptools_research.tasks.report_dataset_validation_result import (
    ReportDatasetValidationResult,
)
from siptools_research.workflow import (
    TARGET_TASKS,
    Workflow,
)
from siptools_research.workflow_init import InitWorkflows


def test_initworkflows(config, requests_mock):
    """Test InitWorkflows task.

    Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces Tasks
    for each incomplete workflow in database.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    for dataset_id in ["dataset1", "dataset2", "dataset3"]:
        dataset = copy.deepcopy(DATASET)
        dataset["id"] = dataset_id
        tests.utils.add_metax_dataset(requests_mock, dataset=dataset)

    # Add sample workflows to database
    Workflow("dataset1", config=config).generate_metadata()
    dataset_2 = Workflow("dataset2", config=config)
    dataset_2.generate_metadata()
    dataset_2.disable()
    Workflow("dataset3", config=config).generate_metadata()

    # Get list of tasks required by InitWorkflows task
    task = InitWorkflows(config=config)
    required_tasks = list(task.requires())

    # Only workflows of dataset1 and dataset3 should be enabled
    assert {required_task.dataset_id for required_task in required_tasks}\
        == {'dataset1', 'dataset3'}


@pytest.mark.parametrize(
    ("method", "target_task"),
    [
        ("preserve", Cleanup),
        ("validate", ReportDatasetValidationResult),
        ("generate_metadata", GenerateMetadata),
    ]
)
def test_init_correct_task(config, requests_mock, method, target_task):
    """Test InitWorkflows requires correct Task.

    Add a workflow to database using one the methods, and check that
    expected Task is required.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param method: The name of the method that is used to initialize
        workflow
    :param target_task: The Task expeceted to be required by
        InitWorkflows
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    # Metadata must be confirmed to allow validation or preservation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    # Add a workflow to database
    workflow = Workflow(dataset_id="test_dataset_id", config=config)
    getattr(workflow, method)()

    task = InitWorkflows(config=config)
    required_tasks = list(task.requires())

    # Check that the expected Task is required
    assert len(required_tasks) == 1
    assert isinstance(required_tasks[0], target_task)


def test_complete_workflow(requests_mock, config):
    """Test that tasks are not required if they are already completed.

    Add a workflow to database. Create a fake output for the target task
    of the workflow. The workflow should not run, because it has already
    been completed.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock)

    # Initialize workflow
    workflow = Workflow("test_dataset_id", config=config)
    workflow.generate_metadata()

    # Create fake output for the target task, so it looks like it would
    # be completed
    target_task = TARGET_TASKS[workflow.target](
        dataset_id=workflow.dataset.identifier,
        config=config
    )
    Path(target_task.output().path).write_text("Fake output for the task")
    assert target_task.complete()  # Just a sanity check

    # Luigi should not run any tasks, i.e. InitWorkflows task should not
    # require any tasks
    task = InitWorkflows(config=config)
    assert list(task.requires()) == []


def test_invalid_dataset(requests_mock, config):
    """Test that workflow is not run, if the dataset is invalid.

    Add a workflow to database. Add an error to the datastet. Workflow
    should not run, because the dataset is invalid.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock metax
    tests.utils.add_metax_dataset(requests_mock)

    # Initialize workflow
    workflow = Workflow(dataset_id="test_dataset_id", config=config)
    workflow.generate_metadata()

    # Add error to dataset
    Dataset(identifier="test_dataset_id",
            config=config).add_error("Fake error")

    # Luigi should not run any tasks, i.e. InitWorkflows task should not
    # require any tasks
    task = InitWorkflows(config=config)
    assert list(task.requires()) == []
