"""Tests for :mod:`siptools_research.dataset` module."""
import datetime

import pytest
from requests_mock import ANY

from siptools_research.exceptions import WorkflowExistsError
from siptools_research.dataset import Dataset, find_datasets
from tests.conftest import UNIT_TEST_CONFIG_FILE


@pytest.mark.parametrize(
    'metadata',
    [
        # Pottumonttu dataset
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-pas'
            },
            'preservation_identifier': 'correct-id',
            
        },
        # Ida dataset
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            },
            'identifier': 'wrong-id',
            'preservation_dataset_version': {
                'preferred_identifier': 'correct-id'
            }
        },
    ]
)
def test_sip_identifier(requests_mock, metadata):
    """Test that dataset returns correct sip_identifier.

    :param metadata: The metadata of dataset from Metax
    """
    requests_mock.get('/rest/v2/datasets/identifier', json=metadata)
    requests_mock.get('/rest/v2/datasets/wrong-id?include_user_metadata=true&file_details=true', json={})
    requests_mock.get('/rest/v2/datasets/wrong-id/files', json={})
    dataset = Dataset('identifier', config=UNIT_TEST_CONFIG_FILE)
    assert dataset.sip_identifier == 'correct-id'


def test_no_sip_identifier(requests_mock):
    """Test that exception is raised if dataset does not have SIP ID."""
    requests_mock.get(
        '/rest/v2/datasets/identifier',
        json={
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            }
        }
    )
    dataset = Dataset('identifier', config=UNIT_TEST_CONFIG_FILE)
    with pytest.raises(ValueError, match='DOI does not exist'):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


@pytest.mark.parametrize(
    'metadata',
    [
        # Pottumonttu dataset
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-pas'
            },
            'preservation_state': 'correct-state',
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            },
            'preservation_state': 'correct-state',
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            },
            'identifier': 'wrong-state',
            'preservation_dataset_version': {
                'preservation_state': 'correct-state'
            },
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        }
    ]
)
def test_preservation_state(requests_mock, metadata):
    """Test that dataset returns correct preservation state.

    :param metadata: The metadata of dataset from Metax
    """
    requests_mock.get('/rest/v2/datasets/identifier', json=metadata)
    dataset = Dataset('identifier', config=UNIT_TEST_CONFIG_FILE)
    assert dataset.preservation_state == 'correct-state'


@pytest.mark.parametrize(
    'metadata',
    [
        # Pottumonttu dataset
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-pas'
            },
            'identifier': 'correct-id',
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            },
            'identifier': 'correct-id',
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            },
            'identifier': 'wrong-id',
            'preservation_dataset_version': {
                'identifier': 'correct-id'
            },
            "research_dataset": {
                "files": [
                    {
                        "details": {
                            "project_identifier": "foo"
                        }
                    }
                ]
            }
        },
    ]
)
def test_set_preservation_state(requests_mock, metadata):
    """Test set_preservation_state method.

    :param metadata: The metadata of dataset from Metax
    """
    # Mock metax
    requests_mock.get('/rest/v2/datasets/identifier', json=metadata)

    # Mock any patch request
    mocked_patch = requests_mock.patch(ANY)

    dataset = Dataset('identifier', config=UNIT_TEST_CONFIG_FILE)
    dataset.set_preservation_state('foo', 'bar')

    # Check that preservation state of correct dataset was set
    assert mocked_patch.last_request.url \
        == 'https://metaksi/rest/v2/datasets/correct-id'

    # Check that the request contains correct message
    assert mocked_patch.last_request.json() == {
        'preservation_state': 'foo',
        'preservation_description': 'bar'
    }


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


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_enable_disable():
    """Test enabling and disabling workflow."""
    # Initially the workflow of the dataset should be disabled
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

    # Check that task was added to task log
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

    Tests that `generate_metadata` sets correct target for workflow of
    the dataset, and creates metadata generation workspace.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).generate_metadata()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'metadata_generation'

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

    Tests that `validate_dataset` sets correct target for workflow of
    the dataset, and creates validation workspace.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).validate()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'validation'

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

    Tests that `prserve_dataset` sets correct target for workflow of
    the dataset, and creates preservation workspace.

    :returns: ``None``
    """
    Dataset('dataset1', config=UNIT_TEST_CONFIG_FILE).preserve()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'preservation'

    # Preservation workspace should be created
    assert dataset.preservation_workspace.exists()


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_restart_preserve_dataset():
    """Test restarting preservation.

    When preservation is restarted, previous preservation workspace
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
    assert not any(dataset.preservation_workspace.iterdir())


@pytest.mark.usefixtures('testmongoclient', 'pkg_root')
def test_workflow_conflict():
    """Test starting another workflow for dataset.

    Tests that new workflows can not be started when dataset
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
    assert active_datasets[0].target.value == 'validation'
    assert active_datasets[0].identifier == 'dataset1'

    # New workflow can be started when the previous workflow is
    # disabled
    dataset.disable()
    dataset.preserve()

    # The target of the workflow should be updated, and workflow
    # should be enabled
    active_datasets = find_datasets(enabled=True, config=UNIT_TEST_CONFIG_FILE)
    assert len(active_datasets) == 1
    assert active_datasets[0].target.value == 'preservation'
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
