"""Tests for :mod:`siptools_research.dataset` module."""
import copy
import datetime

import pytest
from metax_access import (
    DS_STATE_GENERATING_METADATA,
    DS_STATE_IN_PACKAGING_SERVICE,
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_REJECTED_BY_USER,
    DS_STATE_VALIDATING_METADATA,
)
from metax_access.template_data import DATASET, FILE
from siptools_research.dataset import Dataset
from siptools_research.exceptions import (
    AlreadyPreservedError,
    WorkflowExistsError,
)
from siptools_research.models.file_error import FileError
from siptools_research.workflow import Workflow, find_workflows
from tests.utils import add_metax_dataset


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            # Pottumonttu datasets are created in PAS data-catalog, so
            # there is only one version of pottumonttu dataset.
            # Therefore, the persistent identifier is the DOI which
            # should be used as SIP identifier. NOTE: In future there
            # will be separate data-catalog for pottumonttu datasets:
            # TPASPKT-749
            "persistent_identifier": "correct-id"
        },
        # Ida dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": -1,
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": "pas-version-uuid",
                    # This is the DOI of the PAS version of the
                    # dataset, which should, i.e. the identitier of
                    # the SIP
                    "persistent_identifier": "correct-id",
                    "preservation_state": -1
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            }
        },
    ]
)
def test_sip_identifier(config, requests_mock, metadata):
    """Test that dataset returns correct sip_identifier.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    dataset = Dataset(dataset['id'], config=config)
    assert dataset.sip_identifier == "correct-id"


def test_no_sip_identifier(config, requests_mock):
    """Test that exception is raised if dataset does not have SIP ID.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)
    dataset = Dataset(dataset['id'], config=config)
    with pytest.raises(ValueError, match='DOI does not exist'):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


def test_unknown_data_catalog(config, requests_mock):
    """Test that exception is raised if data catalog is unknown.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-unknown"
    requests_mock.get("/v3/datasets/identifier", json=dataset)

    dataset = Dataset("identifier", config=config)
    with pytest.raises(ValueError, match="Unknown data catalog"):
        # pylint: disable=pointless-statement
        dataset.sip_identifier


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            "preservation": {
                "state": "correct-state",
                "description": None,
                "reason_description": None,
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": "correct-state",
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": None,
                    "persistent_identifier": None,
                    "preservation_state": -1,
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "preservation": {
                "state": "wrong-state",
                "description": None,
                "reason_description": None,
                "dataset_version": {
                    "id": "pas-version-id",
                    "persistent_identifier": "pas-version-doi",
                    "preservation_state": "correct-state",
                },
                "contract": "contract_identifier",
                "pas_package_created": None,
            },
        },
    ]
)
def test_preservation_state(config, requests_mock, metadata):
    """Test that dataset returns correct preservation state.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param metadata: The metadata of dataset from Metax
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get('/v3/datasets/identifier', json=dataset)

    dataset = Dataset('identifier', config=config)
    assert dataset.preservation_state == 'correct-state'


@pytest.mark.parametrize(
    "metadata",
    [
        # Pottumonttu dataset
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-pas",
            "id": "correct-id",
        },
        # Ida dataset that has not been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "id": "correct-id",
            "preservation": {
                "dataset_version": None,
                "state": 0,
                "description": None,
                "reason_description": None,
                "contract": None,
                "pas_package_created": None,
            }
        },
        # Ida dataset that has been copied to PAS data catalog
        {
            "data_catalog": "urn:nbn:fi:att:data-catalog-ida",
            "id": "wrong-id",
            "preservation": {
                "dataset_version": {
                    "id": "correct-id",
                    "persistent_identifier": None,
                    "preservation_state": 0
                },
                "state": 0,
                "description": None,
                "reason_description": None,
                "contract": None,
                "pas_package_created": None,
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
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(metadata)
    requests_mock.get("/v3/datasets/identifier", json=dataset)
    mocked_patch = requests_mock.patch("/v3/datasets/correct-id/preservation")

    dataset = Dataset("identifier", config=config)
    dataset.set_preservation_state("foo", "bar")

    # Check that the request contains correct message
    assert mocked_patch.called_once
    assert mocked_patch.last_request.json() == {
        "state": "foo",
        "description": {"en": "bar"}
    }


def test_workspace_paths(config, workspace):
    """Test workspace paths.

    :param config: Configuration file
    :param workspace: Workspace directory
    """
    workflow = Workflow(workspace.name, config=config)
    assert workflow.workspace.root \
        == workspace

    assert workflow.workspace.metadata_generation \
        == workspace / 'metadata_generation'

    assert workflow.workspace.validation \
        == workspace / 'validation'

    assert workflow.workspace.preservation \
        == workspace / 'preservation'


def test_enable_disable(config):
    """Test enabling and disabling workflow.

    :param config: Configuration file
    """
    # Initially the workflow should be disabled
    workflow = Workflow('foo', config=config)
    assert workflow.enabled is False

    # Worklfow can be enabled
    workflow.enable()
    assert workflow.enabled is True

    # Worklfow can be disabled
    workflow.disable()
    assert workflow.enabled is False


def test_task_log(config):
    """Test logging tasks and reading the log.

    :param config: Configuration file
    """
    dataset = Dataset('foo', config=config)

    # Add a task to log
    dataset.log_task('TestTask', 'success',
                     'Everything went better than expected')

    # Check that task was added to task log
    tasks = dataset._document.workflow_tasks
    assert tasks['TestTask']['messages'] \
        == 'Everything went better than expected'
    assert tasks['TestTask']['result'] == 'success'

    # Check that the timestamp of the task is correct format
    timestamp = dataset._document.workflow_tasks["TestTask"]["timestamp"]
    assert timestamp.endswith("+00:00")
    assert datetime.datetime.strptime(
        timestamp[:-6],  # Remove the UTC offset
        '%Y-%m-%dT%H:%M:%S.%f'
    )


def test_generate_metadata(config, requests_mock):
    """Test generate_metadata function.

    Tests that `generate_metadata`

    * sets correct target for workflow of the dataset
    * creates metadata generation workspace.
    * sets preservation state

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    patch_preservation = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    Workflow("test_dataset_id", config=config).generate_metadata()

    # Check that workflow was added to database.
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target.value == 'metadata_generation'

    # Metadata generation workspace should be created
    assert workflow.workspace.metadata_generation.exists()

    # Preservation state should be set
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_GENERATING_METADATA,
        "description": {"en": "File identification started by user"}
    }


def test_restart_generate_metadata(config, requests_mock):
    """Test restarting metadata generation.

    When metadata generation is restarted, previous workspaces should be
    cleared.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    workflow = Workflow("test_dataset_id", config=config)

    # Create preservation workspaces
    workflow.workspace.metadata_generation.mkdir(parents=True)
    (workflow.workspace.metadata_generation / 'test').write_text('foo')
    workflow.workspace.validation.mkdir(parents=True)
    workflow.workspace.preservation.mkdir(parents=True)

    # Restart metadata generation
    workflow.generate_metadata()

    # Previous workspaces should now be cleared
    assert not any(workflow.workspace.metadata_generation.iterdir())
    assert not workflow.workspace.validation.exists()
    assert not workflow.workspace.preservation.exists()


def test_validate_dataset(config, requests_mock):
    """Test validate_dataset function.

    Tests that `validate_dataset`

    * sets correct target for workflow of the dataset
    * creates validation workspace
    * sets preservation state

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)
    patch_preservation = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    Workflow("test_dataset_id", config=config).validate()

    # Check that dataset was added to database.
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target.value == 'validation'

    # Validation workspace should be created
    assert workflow.workspace.validation.exists()

    # Preservation state should be set
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_VALIDATING_METADATA,
        "description": {"en": "Proposed for preservation by user"}
    }


def test_restart_validate_metadata(config, requests_mock):
    """Test restarting validation.

    When validation is restarted, previous validation and preservation
    workspaces should be cleared.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)

    workflow = Workflow("test_dataset_id", config=config)

    # Create workspaces
    workflow.workspace.metadata_generation.mkdir(parents=True)
    (workflow.workspace.metadata_generation/ 'test').write_text('foo')
    workflow.workspace.validation.mkdir(parents=True)
    (workflow.workspace.validation / 'test').write_text('bar')
    workflow.workspace.preservation.mkdir(parents=True)

    # Restart validation
    workflow.validate()

    # Metadata generation workspace still contain files
    assert [file.name
            for file
            in workflow.workspace.metadata_generation.iterdir()] \
        == ['test']

    # Validation workspace should be empty
    assert not any(workflow.workspace.validation.iterdir())

    # Preservation workspace should be removed
    assert not workflow.workspace.preservation.exists()


def test_preserve_dataset(config, requests_mock):
    """Test preserve_dataset function.

    Tests that `prserve_dataset`

    * sets correct target for workflow of the dataset
    * creates preservation workspace
    * sets preservation state

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)
    patch_preservation = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    Workflow("test_dataset_id", config=config).preserve()

    # Check that dataset was added to database.
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target.value == 'preservation'

    # Preservation workspace should be created
    assert workflow.workspace.preservation.exists()

    # Preservation state should be set
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_IN_PACKAGING_SERVICE,
        "description": {"en": "Packaging dataset"}
    }


def test_restart_preserve_dataset(config, requests_mock):
    """Test restarting preservation.

    When preservation is restarted, previous preservation workspace
    should be cleared.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)

    workflow = Workflow("test_dataset_id", config=config)

    # Create workspaces
    workflow.workspace.metadata_generation.mkdir(parents=True)
    (workflow.workspace.metadata_generation / 'test').write_text('foo')
    workflow.workspace.validation.mkdir(parents=True)
    (workflow.workspace.validation / 'test').write_text('bar')
    workflow.workspace.preservation.mkdir(parents=True)
    (workflow.workspace.preservation / 'test').write_text('baz')

    # Restart validation
    workflow.preserve()

    # Metadata generation workspace and validation workspace should
    # still contain files
    assert [file.name
            for file
            in workflow.workspace.metadata_generation.iterdir()] \
        == ['test']
    assert [file.name
            for file
            in workflow.workspace.validation.iterdir()] == ['test']

    # Preservation workspace should be cleaned
    assert not any(workflow.workspace.preservation.iterdir())


def test_confirm_dataset(requests_mock, config):
    """Test confirming dataset metadata.

    Tests that preservation state is updated when dataset metadata is
    confirmed.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)
    patch_preservation = requests_mock.patch(
        f"/v3/datasets/{dataset['id']}/preservation", json={}
    )

    Workflow(dataset["id"], config=config).confirm()

    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_METADATA_CONFIRMED,
        "description": {"en": "Metadata confirmed by user"}
    }


def test_unlock_dataset(requests_mock, config):
    """Test unlocking dataset.

    Tests that `reset` updates Metax state, unlocks the dataset
    and removes any existing file errors

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )
    requests_mock.get(
        "/v3/datasets/test_dataset_id/files",
        json={
            "results": [
                FILE | {
                    "id": f"file-id-{i}",
                    "storage-identifier": f"storage-identifier-{i}"
                } for i in range(1, 3)  # Files 1 and 2
            ],
            "next": None,
            "previous": None
        }
    )
    files_post = requests_mock.post("/v3/files/patch-many")

    # Add dataset error to database
    dataset = Dataset("test_dataset_id", config=config)
    dataset.add_error("Test error message")

    # Add file errors to database
    FileError.objects.insert([
        FileError(
            file_id="file-id-1",
            storage_identifier="storage-identifier-1",
            storage_service="pas",
            dataset_id=None
        ),
        FileError(
            file_id="file-id-2",
            storage_identifier="storage-identifier-2",
            storage_service="pas",
            dataset_id="test_dataset_id"
        ),
        # This one won't be deleted
        FileError(
            file_id="file-id-3",
            storage_identifier="storage-identifier-3",
            storage_service="pas",
        )
    ])

    dataset.unlock()

    # Dataset metadata should be unlocked in Metax
    assert preservation_patch.called_once
    assert preservation_patch.last_request.json()["pas_process_running"] \
        is False

    # Metadata of all files ot the dataset should be unlocked in Metax
    assert files_post.called_once
    assert files_post.last_request.json() == [
        {
            "id": "file-id-1",
            "pas_process_running": False,
        },
        {
            "id": "file-id-2",
            "pas_process_running": False,
        },
    ]

    # Dataset errors should be cleared
    assert dataset.errors == []

    # File errors for the two files are removed
    assert FileError.objects.count() == 1
    assert FileError.objects.all()[0].file_id == "file-id-3"


def test_reset_dataset(requests_mock, config):
    """Test dataset reset function.

    Tests that `reset` updates Metax state, unlocks the dataset.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    requests_mock.post("/v3/files/patch-many", json={})

    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    Workflow("test_dataset_id", config=config).reset(
        description="Reset by user",
        reason_description="Why this dataset was reset"
    )

    # Preservation state set to INITIALIZED and dataset is unlocked
    assert any(
        req for req in preservation_patch.request_history
        if req.json().get("state", None) == DS_STATE_INITIALIZED
    )
    assert any(
        req for req in preservation_patch.request_history
        if req.json().get("pas_process_running", None) is False
    )


def test_reject_dataset(requests_mock, config):
    """Test reject method of Dataset.

    Tests that that preservation state is updated.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    preservation_patch = requests_mock.patch(
        f"/v3/datasets/test_dataset_id/preservation", json={}
    )

    workflow = Workflow("test_dataset_id", config=config)
    workflow.reject()

    # Preservation state should be set
    assert preservation_patch.called_once
    assert preservation_patch.last_request.json() == {
        "state": DS_STATE_REJECTED_BY_USER,
        "description": {"en": "Rejected by user"}
    }


def test_workflow_conflict(config, requests_mock):
    """Test starting another workflow for dataset.

    Tests that new workflows can not be started when dataset
    already has an active workflow.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)

    # Add a sample workflow to database
    workflow = Workflow("test_dataset_id", config=config)
    workflow.validate()

    # Try to start another workflow
    with pytest.raises(WorkflowExistsError):
        workflow.preserve()

    # New workflows should not be created and the existing workflow
    # should not be changed
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    assert active_workflows[0].target.value == 'validation'
    assert active_workflows[0].dataset.identifier == "test_dataset_id"

    # New workflow can be started when the previous workflow is
    # disabled
    workflow.disable()
    workflow.preserve()

    # The target of the workflow should be updated, and workflow
    # should be enabled
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    assert active_workflows[0].target.value == 'preservation'
    assert active_workflows[0].dataset.identifier == "test_dataset_id"


@pytest.mark.parametrize(
    ["method_name", "arguments"],
    [
        ("generate_metadata", ()),
        ("validate", ()),
        ("preserve", ()),
        ("reset", ("foo", "bar")),
    ]
)
def test_already_preserved(config, requests_mock, method_name, arguments):
    """Test that preserved dataset can not be preserved again.

    Initializing any workflow for dataset that has already been
    preserved is not allowed. Exception should be raised.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param method_name: Name of the method that will be tested
    :param arguments: Arguments for the method
    """
    # Mock Metax. Create a dataset that is already in preservation.
    dataset_metadata = copy.deepcopy(DATASET)
    dataset_metadata["preservation"]["pas_package_created"] = True
    add_metax_dataset(requests_mock, dataset=dataset_metadata)

    workflow = Workflow("test_dataset_id", config=config)
    method = getattr(workflow, method_name)

    with pytest.raises(AlreadyPreservedError):
        method(*arguments)


@pytest.mark.parametrize(
    ("kwargs", "expected_datasets"),
    [
        # Search without parameters should return all datasets
        ({}, ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6"]),
        # Only enabled datasets
        ({"enabled": True}, ["ds1", "ds3", "ds5"]),
        # Only disabled  datasets
        ({"enabled": False}, ["ds2", "ds4", "ds6"]),
    ]
)
def test_find_workflows(config, requests_mock, kwargs, expected_datasets):
    """Test find_workflows function.

    Check that find_workflows finds correct workflows.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param kwargs: Keyword arguments to be used
    :param expected_datasets: Identifiers of datasets that should be
                              found
    """
    # Mock Metax
    for dataset_id in ["ds1", "ds2", "ds3", "ds4", "ds5", "ds6"]:
        metadata = copy.deepcopy(DATASET)
        metadata["id"] = dataset_id
        add_metax_dataset(requests_mock=requests_mock, dataset=metadata)

    # Add some workflows to database
    workflow1 = Workflow('ds1', config=config)
    workflow1.generate_metadata()
    workflow2 = Workflow('ds2', config=config)
    workflow2.generate_metadata()
    workflow2.disable()
    workflow3 = Workflow('ds3', config=config)
    workflow3.validate()
    workflow4 = Workflow('ds4', config=config)
    workflow4.validate()
    workflow4.disable()
    workflow5 = Workflow('ds5', config=config)
    workflow5.preserve()
    workflow6 = Workflow('ds6', config=config)
    workflow6.preserve()
    workflow6.disable()

    # Check that there is no extra workflows in database
    dataset_identifiers = [
        workflow.dataset.identifier
        for workflow
        in find_workflows(**kwargs, config=config)
    ]

    assert dataset_identifiers == expected_datasets
