"""Tests for :mod:`siptools_research.workflow_init` module."""
import pytest

from siptools_research.dataset import (
    Dataset,
    Target,
)
from siptools_research.workflow.cleanup import Cleanup
from siptools_research.workflow.generate_metadata import GenerateMetadata
from siptools_research.workflow_init import InitWorkflows
from siptools_research.workflow.report_dataset_validation_result import \
    ReportDatasetValidationResult


@pytest.mark.usefixtures("testmongoclient")
def test_initworkflows(config):
    """Test InitWorkflows task.

    Add few sample workflows to database and test that
    ``InitWorkflows.requires`` function produces Tasks
    for each incomplete workflow in database.

    :param config: Configuration file
    """
    # Add sample workflows to database
    Dataset('dataset1', config=config).preserve()
    dataset_2 = Dataset('dataset2', config=config)
    dataset_2.preserve()
    dataset_2.disable()
    Dataset('dataset3', config=config).preserve()

    # Get list of tasks required by InitWorkflows task
    task = InitWorkflows(config=config)
    required_tasks = list(task.requires())

    # Only workflows of dataset1 and dataset3 should be enabled
    assert {required_task.dataset_id for required_task in required_tasks}\
        == {'dataset1', 'dataset3'}


@pytest.mark.usefixtures("testmongoclient")
@pytest.mark.parametrize(
    ("target", "target_task"),
    [
        ('preservation', Cleanup),
        ('validation', ReportDatasetValidationResult),
        ('metadata_generation', GenerateMetadata),
    ]
)
def test_init_correct_task(config, target, target_task):
    """Test InitWorkflows requires correct Task.

    Add a workflow to database and check that expected Task is required.

    :param config: Configuration file
    :param target: The target of the workflow that is added to databse
    :param target_task: The Task expeceted to be required by
                        InitWorkflows
    """
    # Add a workflow to database
    dataset = Dataset('dataset1', config=config)
    # pylint: disable=protected-access
    dataset._set_target(Target(target))
    dataset.enable()

    task = InitWorkflows(config=config)
    required_tasks = list(task.requires())

    # Check that the expected Task is required
    assert len(required_tasks) == 1
    assert isinstance(required_tasks[0], target_task)

    # The required task should be the "target task"
    assert required_tasks[0].is_target_task
