"""Tests for :mod:`siptools_research.dataset` module."""
import copy
import datetime

import pytest
from requests_mock import ANY

from siptools_research.dataset import Dataset, find_datasets
from siptools_research.exceptions import WorkflowExistsError
from tests.metax_data.datasetsV3 import BASE_DATASET


@pytest.mark.parametrize(
    ("metadata", "v2_metadata"),
    [
        # Pottumonttu dataset
        (
            # Metax v3
            {
                "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
                # Pottumonttu datasets are created in PAS data-catalog,
                # so there is only one version of pottumonttu dataset.
                # Therefore, the persistent identifier is the DOI which
                # should be used as SIP identifier.
                # NOTE: In future there will be separate data-catalog
                # for pottumonttu files: TPASPKT-749
                "persistent_identifier": "correct-id"
            },
            # Metax v2
            {
                "data_catalog": {
                    "identifier": "urn:nbn:fi:att:data-catalog-pas"
                },
                "research_dataset": {
                    "preferred_identifier": "correct-id"
                },
                # In pottumonttu datasets
                # `research_dataset.preferred_identifier` and
                # `preservation_identifier` always have the same value,
                # so both could be used as SIP identifier.
                "preservation_identifier": "correct-id-but-not-used",
            },
        ),
        # Ida dataset
        (
            # Metax v3
            {
                "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
                "preservation": {
                    "id": None,  # TODO: this should not be required
                    "state": -1,
                    "description": None,
                    "reason_description": None,
                    "dataset_version": {
                        "id": None, #uuid
                        # This is the DOI of the PAS version of the
                        # dataset, which should, i.e. the identitier of
                        # the SIP
                        "persistent_identifier": "correct-id",
                        "preservation_state": -1
                    },
                    "contract": "contract_identifier",
                }
            },
            # Metax v2
            {
                "data_catalog": {
                    "identifier": "urn:nbn:fi:att:data-catalog-ida"
                },
                "identifier": "wrong-id",
                "preservation_dataset_version": {
                    "preferred_identifier": "correct-id"
                }
            },
        )
    ]
)
def test_sip_identifier(config, requests_mock, metadata, v2_metadata):
    """Test that dataset returns correct sip_identifier.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax API V3
    dataset = copy.deepcopy(BASE_DATASET)
    dataset.update(metadata)
    requests_mock.get(
        "/v3/datasets/identifier?include_nulls=True",
        json=dataset
    )

    # Mock Metax API V2
    requests_mock.get("/rest/v2/datasets/identifier", json=v2_metadata)
    requests_mock.get("/rest/v2/datasets/wrong-id", json={})
    requests_mock.get("/rest/v2/datasets/wrong-id/files", json={})

    dataset = Dataset("identifier", config=config)
    assert dataset.sip_identifier == "correct-id"


def test_no_sip_identifier(config, requests_mock):
    """Test that exception is raised if dataset does not have SIP ID.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax API v3
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    requests_mock.get( "/v3/datasets/identifier", json=dataset)

    # Mock Metax API v2
    requests_mock.get(
        '/rest/v2/datasets/identifier',
        json={
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-ida'
            }
        }
    )

    dataset = Dataset('identifier', config=config)
    with pytest.raises(ValueError, match='DOI does not exist'):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


def test_unknown_data_catalog(config, requests_mock):
    """Test that exception is raised if data catalog is unknown.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax API v3
    dataset = copy.deepcopy(BASE_DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-unknown"
    requests_mock.get( "/v3/datasets/identifier", json=dataset)

    # Mock Metax API v2
    requests_mock.get(
        '/rest/v2/datasets/identifier',
        json={
            'data_catalog': {
                'identifier': 'urn:nbn:fi:att:data-catalog-unknown'
            }
        }
    )

    dataset = Dataset("identifier", config=config)
    with pytest.raises(ValueError, match="Unknown data catalog"):
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
def test_preservation_state(config, requests_mock, metadata):
    """Test that dataset returns correct preservation state.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    requests_mock.get('/rest/v2/datasets/identifier', json=metadata)
    dataset = Dataset('identifier', config=config)
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
def test_set_preservation_state(config, requests_mock, metadata):
    """Test set_preservation_state method.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock metax
    requests_mock.get('/rest/v2/datasets/identifier', json=metadata)

    # Mock any patch request
    mocked_patch = requests_mock.patch(ANY)

    dataset = Dataset('identifier', config=config)
    dataset.set_preservation_state('foo', 'bar')

    # Check that preservation state of correct dataset was set
    assert mocked_patch.last_request.url \
        == "https://metax.localhost/rest/v2/datasets/correct-id"

    # Check that the request contains correct message
    assert mocked_patch.last_request.json() == {
        'preservation_state': 'foo',
        'preservation_description': 'bar'
    }


def test_workspace_paths(config, workspace):
    """Test workspace paths.

    :param config: Configuration file
    :param workspace: Workspace directory
    """
    dataset = Dataset(workspace.name, config=config)
    assert dataset.workspace_root \
        == workspace

    assert dataset.metadata_generation_workspace \
        == workspace / 'metadata_generation'

    assert dataset.validation_workspace \
        == workspace / 'validation'

    assert dataset.preservation_workspace \
        == workspace / 'preservation'


@pytest.mark.usefixtures("testmongoclient")
def test_enable_disable(config):
    """Test enabling and disabling workflow.

    :param config: Configuration file
    """
    # Initially the workflow of the dataset should be disabled
    dataset = Dataset('foo', config=config)
    assert dataset.enabled is False

    # Worklfow can be enabled
    dataset.enable()
    assert dataset.enabled is True

    # Worklfow can be disabled
    dataset.disable()
    assert dataset.enabled is False


@pytest.mark.usefixtures('testmongoclient')
def test_task_log(config):
    """Test logging tasks and reading the log.

    :param config: Configuration file
    """
    dataset = Dataset('foo', config=config)

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
    assert len(find_datasets(config=config)) == 1


@pytest.mark.usefixtures("testmongoclient")
def test_generate_metadata(config):
    """Test generate_metadata function.

    Tests that `generate_metadata` sets correct target for workflow of
    the dataset, and creates metadata generation workspace.

    :param config: Configuration file
    """
    Dataset('dataset1', config=config).generate_metadata()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=config)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'metadata_generation'

    # Metadata generation workspace should be created
    assert dataset.metadata_generation_workspace.exists()


@pytest.mark.usefixtures("testmongoclient")
def test_restart_generate_metadata(config):
    """Test restarting metadata generation.

    When metadata generation is restarted, previous workspaces should be
    cleared.

    :param config: Configuration file
    """
    dataset = Dataset('dataset1', config=config)

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


@pytest.mark.usefixtures("testmongoclient")
def test_validate_dataset(config):
    """Test validate_dataset function.

    Tests that `validate_dataset` sets correct target for workflow of
    the dataset, and creates validation workspace.

    :param config: Configuration file
    """
    Dataset('dataset1', config=config).validate()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=config)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'validation'

    # Validation workspace should be created
    assert dataset.validation_workspace.exists()


@pytest.mark.usefixtures("testmongoclient")
def test_restart_validate_metadata(config):
    """Test restarting validation.

    When validation is restarted, previous validation and preservation
    workspaces should be cleared.

    :param config: Configuration file
    """
    dataset = Dataset('dataset1', config=config)

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


@pytest.mark.usefixtures("testmongoclient")
def test_preserve_dataset(config):
    """Test preserve_dataset function.

    Tests that `prserve_dataset` sets correct target for workflow of
    the dataset, and creates preservation workspace.

    :param config: Configuration file
    """
    Dataset('dataset1', config=config).preserve()

    # Check that dataset was added to database.
    active_datasets = find_datasets(enabled=True, config=config)
    assert len(active_datasets) == 1
    dataset = active_datasets[0]
    assert dataset.identifier == 'dataset1'
    assert dataset.target.value == 'preservation'

    # Preservation workspace should be created
    assert dataset.preservation_workspace.exists()


@pytest.mark.usefixtures("testmongoclient")
def test_restart_preserve_dataset(config):
    """Test restarting preservation.

    When preservation is restarted, previous preservation workspace
    should be cleared.

    :param config: Configuration file
    """
    dataset = Dataset('dataset1', config=config)

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


@pytest.mark.usefixtures("testmongoclient")
def test_workflow_conflict(config):
    """Test starting another workflow for dataset.

    Tests that new workflows can not be started when dataset
    already has an active workflow.

    :param config: Configuration file
    """
    # Add a sample workflow to database
    dataset = Dataset("dataset1", config=config)
    dataset.validate()

    # Try to start another workflow
    with pytest.raises(WorkflowExistsError):
        dataset.preserve()

    # New workflows should not be created and the existing workflow
    # should not be changed
    active_datasets = find_datasets(enabled=True, config=config)
    assert len(active_datasets) == 1
    assert active_datasets[0].target.value == 'validation'
    assert active_datasets[0].identifier == 'dataset1'

    # New workflow can be started when the previous workflow is
    # disabled
    dataset.disable()
    dataset.preserve()

    # The target of the workflow should be updated, and workflow
    # should be enabled
    active_datasets = find_datasets(enabled=True, config=config)
    assert len(active_datasets) == 1
    assert active_datasets[0].target.value == 'preservation'
    assert active_datasets[0].identifier == 'dataset1'


@pytest.mark.parametrize(
    ("kwargs", "expected_datasets"),
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
@pytest.mark.usefixtures("testmongoclient")
def test_find_datasets(config, kwargs, expected_datasets):
    """Test find_datasets function.

    Check that find_datasets finds correct datasets.

    :param config: Configuration file
    :param kwargs: Keyword arguments to be used
    :param expected_datasets: Identifiers of datasets that should be
                              found
    """
    # Add some datasets to database
    ds1 = Dataset('ds1', config=config)
    ds1.generate_metadata()
    ds2 = Dataset('ds2', config=config)
    ds2.generate_metadata()
    ds2.disable()
    ds3 = Dataset('ds3', config=config)
    ds3.validate()
    ds4 = Dataset('ds4', config=config)
    ds4.validate()
    ds4.disable()
    ds5 = Dataset('ds5', config=config)
    ds5.preserve()
    ds6 = Dataset('ds6', config=config)
    ds6.preserve()
    ds6.disable()

    # Check that there is no extra workflows in database
    dataset_identifiers = [
        dataset.identifier
        for dataset
        in find_datasets(**kwargs, config=config)
    ]

    assert dataset_identifiers == expected_datasets
