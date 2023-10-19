"""Tests for :mod:`siptools_research.dataset` module."""
import datetime

import pytest

from siptools_research.exceptions import WorkflowExistsError
from siptools_research.dataset import Dataset, find_datasets
from tests.conftest import UNIT_TEST_CONFIG_FILE


def test_workspace_paths(pkg_root):
    """Test workspace paths."""
    dataset = Dataset('foo', config=UNIT_TEST_CONFIG_FILE)
    assert dataset.workspace_root \
        == pkg_root / 'workspaces' / 'foo'

    assert dataset.metadata_generation_workspace \
        == pkg_root / 'workspaces' / 'foo' / 'metadata_generation'

    assert dataset.validation_workspace \
        == pkg_root / 'workspaces' / 'foo' / 'validation'

    assert dataset.preservation_workspace \
        == pkg_root / 'workspaces' / 'foo' / 'preservation'

    assert dataset.sip_creation_path \
        == pkg_root / 'workspaces' / 'foo' / 'preservation' / 'sip-in-progress'


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_enable_disable():
    """Test enabling and disabling workflow."""
    # Initially the workflow should be disabled
    dataset = Dataset('foo', config=UNIT_TEST_CONFIG_FILE)
    assert dataset.enabled is False

    # Worklfow can be enabled
    dataset.enable()
    assert dataset.enabled is True

    # Worklfow can be disabled
    dataset.disable()
    assert dataset.enabled is False


@pytest.mark.usefixtures('testmongoclient')
def test_task_log():
    """Test logging tasks and reading the log.

    :returns: ``None``
    """
    dataset = Dataset('foo', config=UNIT_TEST_CONFIG_FILE)

    # Add a task to log
    dataset.log_task('TestTask', 'success',
                     'Everything went better than expected')

    # Check that task was added to workflow
    tasks = dataset.get_tasks()
    assert tasks['TestTask']['messages'] \
        == 'Everything went better than expected'
    assert tasks['TestTask']['result'] == 'success'

    # Check that the timestamp of the task is correct format
    timestamp = dataset.get_task_timestamp('TestTask')
    assert timestamp.endswith("+00:00")
    assert datetime.datetime.strptime(
        timestamp[:-6],  # Remove the UTC offset
        '%Y-%m-%dT%H:%M:%S.%f'
    )

    # Check that there is no extra workflows in database
    assert len(find_datasets(config=UNIT_TEST_CONFIG_FILE)) == 1


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_generate_metadata():
    """Test generate_metadata function.

    Tests that `generate_metadata` schedules correct task.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).generate_metadata()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target == 'metadata_generation'

    # Metadata generation workspace should be created
    assert dataset.metadata_generation_workspace.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_restart_generate_metadata():
    """Test restarting metadata generation.

    When metadata generation is restarted, previous workspaces should be
    cleared.

    :returns: ``None``
    """
    dataset = Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE)

    # Create preservation workspaces
    dataset.metadata_generation_workspace.mkdir(parents=True)
    (dataset.metadata_generation_workspace / 'test').write_text('foo')
    dataset.validation_workspace.mkdir(parents=True)
    dataset.preservation_workspace.mkdir(parents=True)

    # Restart metadata generation
    dataset.generate_metadata()

    # Previous workspaces should now be cleared
    assert not any(dataset.metadata_generation_workspace.iterdir())
    assert not dataset.validation_workspace.exists()
    assert not dataset.preservation_workspace.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_validate_dataset():
    """Test validate_dataset function.

    Tests that `validate_dataset` schedules correct task.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).validate()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target == 'validation'

    # Validation workspace should be created
    assert dataset.validation_workspace.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_restart_validate_metadata():
    """Test restarting validation.

    When validation is restarted, previous validation and preservation
    workspaces should be cleared.

    :returns: ``None``
    """
    dataset = Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE)

    # Create workspaces
    dataset.metadata_generation_workspace.mkdir(parents=True)
    (dataset.metadata_generation_workspace / 'test').write_text('foo')
    dataset.validation_workspace.mkdir(parents=True)
    (dataset.validation_workspace / 'test').write_text('bar')
    dataset.preservation_workspace.mkdir(parents=True)

    # Restart validation
    dataset.validate()

    # Metadata generation workspace still contain files
    assert [file.name
            for file
            in dataset.metadata_generation_workspace.iterdir()] \
        == ['test']

    # Validation workspace should be empty
    assert not any(dataset.validation_workspace.iterdir())

    # Preservation workspace should be removed
    assert not dataset.preservation_workspace.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_preserve_dataset():
    """Test preserve_dataset function.

    Tests that `preserve_dataset` schedules correct task.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).preserve()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target == 'preservation'

    # Preservation workspace should be created
    assert dataset.preservation_workspace.exists()
    assert dataset.sip_creation_path.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_restart_preserve_dataset():
    """Test restarting preservation.

    When preservation is restarted, previous  preservation workspace
    should be cleared.

    :returns: ``None``
    """
    dataset = Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE)

    # Create workspaces
    dataset.metadata_generation_workspace.mkdir(parents=True)
    (dataset.metadata_generation_workspace / 'test').write_text('foo')
    dataset.validation_workspace.mkdir(parents=True)
    (dataset.validation_workspace / 'test').write_text('bar')
    dataset.preservation_workspace.mkdir(parents=True)
    (dataset.preservation_workspace / 'test').write_text('baz')
    dataset.sip_creation_path.mkdir()
    (dataset.sip_creation_path / 'test').write_text('spam')

    # Restart validation
    dataset.preserve()

    # Metadata generation workspace and validation workspace should
    # still contain files
    assert [file.name
            for file
            in dataset.metadata_generation_workspace.iterdir()] \
        == ['test']
    assert [file.name
            for file
            in dataset.validation_workspace.iterdir()] == ['test']

    # Preservation workspace should be cleaned
    assert [path.name
            for path
            in dataset.preservation_workspace.iterdir()] \
        == ['sip-in-progress']
    assert not any(dataset.sip_creation_path.iterdir())


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_workflow_conflict():
    """Test starting another workflow for dataset.

    Tests that that new workflows can not be started when dataset
    already has an active workflow.

    :returns: ``None``
    """
    # Add a sample workflow to database
    dataset = Dataset("dataset1", config=UNIT_TEST_CONFIG_FILE)
    dataset.validate()

    # Try to start another workflow
    with pytest.raises(WorkflowExistsError):
        dataset.preserve()

    # New workflows should not be created and the existing workflow
    # should not be changed
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    assert active_datasets[0].target == 'validation'
    assert active_datasets[0].identifier == 'dataset1'

    # New workflow can be started when the previous workflow is
    # disabled
    dataset.disable()
    dataset.preserve()

    # The target_task of the workflow should be updated, and workflow
    # should be enabled
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    assert active_datasets[0].target == 'preservation'
    assert active_datasets[0].identifier == 'dataset1'


@pytest.mark.parametrize(
    'kwargs,expected_datasets',
    [
        # Search without parameters should return all datasets
        ({}, ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6"]),
        # Only enabled datasets
        ({"enabled": True}, ["ds1", "ds3", "ds5"]),
        # Only disabled  datasets
        ({"enabled": False}, ["ds2", "ds4", "ds6"]),
        # Dataset that are being preserved
        ({"target": "preservation"}, ["ds5", "ds6"]),
        # Enabled datasets that are being preserved
        ({"target": "preservation", "enabled": True}, ["ds5"]),
    ]
)
@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_find_datasets(kwargs, expected_datasets):
    """Test find_datasets function.

    Check that find_datasets finds correct datasets.

    :param kwargs: Keyword arguments to be used
    :param expected_datasets: Identifiers of datasets that should be
                              found
    :returns: ``None``
    """
    # Add some datasets to database
    ds1 = Dataset('ds1', config=UNIT_TEST_CONFIG_FILE)
    ds1.generate_metadata()
    ds2 = Dataset('ds2', config=UNIT_TEST_CONFIG_FILE)
    ds2.generate_metadata()
    ds2.disable()
    ds3 = Dataset('ds3', config=UNIT_TEST_CONFIG_FILE)
    ds3.validate()
    ds4 = Dataset('ds4', config=UNIT_TEST_CONFIG_FILE)
    ds4.validate()
    ds4.disable()
    ds5 = Dataset('ds5', config=UNIT_TEST_CONFIG_FILE)
    ds5.preserve()
    ds6 = Dataset('ds6', config=UNIT_TEST_CONFIG_FILE)
    ds6.preserve()
    ds6.disable()

    # Check that there is no extra workflows in database
    dataset_identifiers = [
        dataset.identifier
        for dataset
        in find_datasets(**kwargs, config=UNIT_TEST_CONFIG_FILE)
    ]

    assert dataset_identifiers == expected_datasets
