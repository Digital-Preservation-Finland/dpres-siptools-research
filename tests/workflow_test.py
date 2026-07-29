"""Tests for :mod:`siptools_research.workflow` module."""
import copy

import pytest
from metax_access import (
    DS_STATE_GENERATING_METADATA,
    DS_STATE_IN_PACKAGING_SERVICE,
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_REJECTED_BY_USER,
    DS_STATE_VALIDATING_METADATA,
)
from metax_access.template_data import DATASET

from siptools_research.exceptions import (
    AlreadyPreservedError,
    DatasetHasErrorsError,
    MetadataAlreadyConfirmedError,
    MetadataNotGeneratedError,
    WorkflowExistsError,
)
from siptools_research.models.workflow_entry import WorkflowEntry
from siptools_research.workflow import (
    Workflow,
    find_workflows,
)
from tests.utils import add_metax_dataset


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


def test_pause_resume(config):
    """Test pausing and resuming workflow.

    :param config: Configuration file
    """
    # Initially the workflow should not be paused
    workflow = Workflow('foo', config=config)
    assert workflow.paused is False

    # Worklfow can be paused
    workflow.pause()
    assert workflow.paused is True

    # Worklfow can be resumed
    workflow.resume()
    assert workflow.paused is False


def test_active(config, requests_mock, workspace):
    """Test checking if workflow is active.

    :param config: Configuration file
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    add_metax_dataset(requests_mock, dataset=dataset)

    # Workflow should be active once it is created
    workflow = Workflow(workspace.name, config=config)
    workflow.generate_metadata()
    assert workflow.active is True

    # Create fake fake output file for the target task. The worfklow
    # should not be active once the target task is complete.
    output_path = (workspace
                   / "metadata_generation"
                   / "generate-metadata.finished")
    output_path.write_text("foo")
    assert workflow.active is False


def test_active_errors(config, requests_mock, workspace):
    """Test that workflow of invalid dataset is not active.

    :param config: Configuration file
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    add_metax_dataset(requests_mock, dataset=dataset)

    workflow = Workflow(workspace.name, config=config)
    workflow.generate_metadata()

    # Add error to dataset. The dataset should not be active.
    workflow.dataset.add_error("Something wrong with the dataset")
    assert workflow.active is False

    # Unlock the dataset to clear errors. The workflow should be active
    # again.
    workflow.dataset.unlock()
    assert not workflow.dataset.errors
    assert workflow.active is True


def test_generate_metadata(config, requests_mock):
    """Test metadata generation.

    Tests that `generate_metadata` method:

    * locks metadata in Metax
    * sets correct target for the workflow
    * creates metadata generation workspace
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

    # Metadata should be locked
    assert patch_preservation.request_history[0].json() \
        == {"pas_process_running": True}

    # Check that workflow was added to database.
    active_workflows = find_workflows(active=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target == "metadata_generation"

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

    # Confirm dataset before restart
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()
    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.metadata_confirmed

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

    # Dataset metadata confirmation should be revoked
    assert workflow.metadata_confirmed is False


def test_preserve(config, requests_mock):
    """Test preservation.

    Tests that `preserve` method:

    * sets correct target for the workflow
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

    # Metadata must be confirmed before preservation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config).preserve()

    # Check that dataset was added to database.
    active_workflows = find_workflows(active=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target == "preservation"

    # Preservation workspace should be created
    assert workflow.workspace.preservation.exists()

    # Preservation state should be set
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_IN_PACKAGING_SERVICE,
        "description": {"en": "Packaging dataset"}
    }


def test_restart_preserve(config, requests_mock):
    """Test restarting preservation.

    When preservation is restarted, previous preservation workspace
    should be cleared.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)

    # Metadata must be confirmed before preservation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    # Create workspaces
    workflow = Workflow("test_dataset_id", config=config)
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


def test_confirm(requests_mock, config, workspace):
    """Test confirming dataset metadata.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)
    patch_preservation = requests_mock.patch(
        f"/v3/datasets/{dataset['id']}/preservation", json={}
    )

    # Add a fake output for metadata generation workflow, so it looks
    # like metadata would be generated
    output_path = (workspace
     / "metadata_generation"
     / "generate-metadata.finished")

    output_path.parent.mkdir()
    output_path.write_text("foo")

    workflow = Workflow(dataset["id"], config=config)
    workflow.confirm()

    assert workflow.metadata_confirmed is True

    # TODO: Setting preservation state is not necessary once
    # TPASPKT-1600 has been resolved.
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_METADATA_CONFIRMED,
        "description": {"en": "Metadata confirmed by user"}
    }


def test_confirm_metadata_not_generated(requests_mock, config):
    """Test confirming dataset metadata when metadata is not generated.

    Exception should be raised.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    with pytest.raises(MetadataNotGeneratedError):
        Workflow(dataset["id"], config=config).confirm()


def test_preserve_dataset_has_errors(config, requests_mock):
    """Test that preservation is blocked when dataset has errors.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    add_metax_dataset(requests_mock)

    # Metadata must be confirmed before preservation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config)
    workflow.dataset.add_error("Something is wrong")

    with pytest.raises(DatasetHasErrorsError):
        workflow.preserve()


def test_confirm_dataset_has_errors(config, requests_mock, workspace):
    """Test that confirming metadata is blocked when dataset has errors.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param workspace: Workspace directory
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    # Add a fake output for metadata generation so it looks complete
    output_path = (workspace
                   / "metadata_generation"
                   / "generate-metadata.finished")
    output_path.parent.mkdir()
    output_path.write_text("foo")

    workflow = Workflow(dataset["id"], config=config)
    workflow.dataset.add_error("Something is wrong")

    with pytest.raises(DatasetHasErrorsError):
        workflow.confirm()


def test_confirm_workflow_active(config, requests_mock, workspace):
    """Test that confirming metadata is blocked while workflow is running.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param workspace: Workspace directory
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    # Add a fake output for metadata generation so it looks complete
    output_path = (workspace
                   / "metadata_generation"
                   / "generate-metadata.finished")
    output_path.parent.mkdir()
    output_path.write_text("foo")

    # Simulate a running validation workflow
    workflow_entry = WorkflowEntry(id=workspace.name)
    workflow_entry.target = "validation"
    workflow_entry.save()

    workflow = Workflow(workspace.name, config=config)
    assert workflow.active is True

    with pytest.raises(WorkflowExistsError):
        workflow.confirm()


def test_confirm_already_confirmed(config, requests_mock, workspace):
    """Test that confirmed metadata can not be confirmed again.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param workspace: Workspace directory
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{dataset['id']}", json=dataset)

    # Add a fake output for metadata generation so it looks complete
    output_path = (workspace
                   / "metadata_generation"
                   / "generate-metadata.finished")
    output_path.parent.mkdir()
    output_path.write_text("foo")

    # Metadata has already been confirmed
    workflow_entry = WorkflowEntry(id=workspace.name)
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    with pytest.raises(MetadataAlreadyConfirmedError):
        Workflow(workspace.name, config=config).confirm()


def test_propose_dataset_has_errors(config, requests_mock):
    """Test that proposing for preservation is blocked when dataset has errors.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    add_metax_dataset(requests_mock)

    # Metadata must be confirmed before proposing
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config)
    workflow.dataset.add_error("Something is wrong")

    with pytest.raises(DatasetHasErrorsError):
        workflow.propose()


def test_reset(requests_mock, config):
    """Test resetting workflow.

    Tests that resetting workflow

    * updates preservation state of dataset in Metax
    * unlocks dataset metadata in Metax
    * resets state of the workflow

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    # Confirm dataset metadata and propose dataset for preservation
    # before restart
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.proposed = True
    workflow_entry.save()
    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.metadata_confirmed is True
    assert workflow.proposed is True

    workflow.reset(
        description="Reset by user",
        reason_description="Why this dataset was reset"
    )

    # Preservation state is set to INITIALIZED and dataset is unlocked
    assert any(
        req for req in preservation_patch.request_history
        if req.json().get("state", None) == DS_STATE_INITIALIZED
    )
    assert any(
        req for req in preservation_patch.request_history
        if req.json().get("pas_process_running", None) is False
    )

    # As the dataset is unlocked, the metadata might change. The
    # metadata confirmation must therefore be revoked.
    assert workflow.metadata_confirmed is False

    # Proposal should be cancelled
    assert workflow.proposed is False

    # Target should be removed
    assert workflow.target is None

    # Workflow should not be active
    assert workflow.active is False

    # Workflow should be removed from database, so the workflow database
    # should be empty
    assert not find_workflows(config=config)


def test_reset_clears_workspaces(requests_mock, config):
    """Test that resetting workflow clears the workspaces.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)

    workflow = Workflow("test_dataset_id", config=config)

    # Create workspaces
    workflow.workspace.metadata_generation.mkdir(parents=True)
    (workflow.workspace.metadata_generation / 'test').write_text('foo')
    workflow.workspace.validation.mkdir(parents=True)
    (workflow.workspace.validation / 'test').write_text('bar')
    workflow.workspace.preservation.mkdir(parents=True)
    (workflow.workspace.preservation / 'test').write_text('baz')

    workflow.reset(
        description="Reset by user",
        reason_description="Why this dataset was reset"
    )

    # All workspaces should be removed
    assert not workflow.workspace.metadata_generation.exists()
    assert not workflow.workspace.validation.exists()
    assert not workflow.workspace.preservation.exists()


def test_propose(requests_mock, config):
    """Test proposing dataset for digital preservation.

    Tests that `propose` method:

    * sets proposed flag to `True`
    * sets correct target for the workflow
    * creates validation workspace
    * sets preservation state

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)

    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    # Metadata must be confirmed before dataset can be proposed for
    # preservation.
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.proposed is False

    workflow.propose()
    assert workflow.proposed is True

    # Check that dataset was added to database.
    active_workflows = find_workflows(active=True, config=config)
    assert len(active_workflows) == 1
    workflow = active_workflows[0]
    assert workflow.dataset.identifier == "test_dataset_id"
    assert workflow.target == "validation"

    # Validation workspace should be created
    assert workflow.workspace.validation.exists()

    # Preservation state should be set
    assert preservation_patch.last_request.json() == {
        "state": DS_STATE_VALIDATING_METADATA,
        "description": {"en": "Proposed for preservation by user"}
    }


def test_restart_validate(config, requests_mock):
    """Test restarting validation.

    When dataset is proposed for preservation, previous validation and
    preservation workspaces should be cleared.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    add_metax_dataset(requests_mock)

    # Metadata must be confirmed before validation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config)

    # Create workspaces
    workflow.workspace.metadata_generation.mkdir(parents=True)
    (workflow.workspace.metadata_generation / 'test').write_text('foo')
    workflow.workspace.validation.mkdir(parents=True)
    (workflow.workspace.validation / 'test').write_text('bar')
    workflow.workspace.preservation.mkdir(parents=True)

    # Propose dataset for preservation to restart validation
    workflow.propose()

    # Metadata generation workspace still contain files
    assert [file.name
            for file
            in workflow.workspace.metadata_generation.iterdir()] \
        == ['test']

    # Validation workspace should be empty
    assert not any(workflow.workspace.validation.iterdir())

    # Preservation workspace should be removed
    assert not workflow.workspace.preservation.exists()


def test_reject(requests_mock, config):
    """Test rejecting workflow.

    Tests that that preservation state is updated.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)
    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    # Dataset must be proposed for preservation before it can be
    # rejected
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.proposed = True
    workflow_entry.save()

    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.proposed is True
    workflow.reject()

    # Proposal should be cancelled
    assert workflow.proposed is False

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
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.target = "metadata_generation"
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    # Try to start another workflow
    workflow = Workflow("test_dataset_id", config=config)
    with pytest.raises(WorkflowExistsError):
        workflow.preserve()

    # New workflows should not be created and the existing workflow
    # should not be changed
    active_workflows = find_workflows(active=True, config=config)
    assert len(active_workflows) == 1
    assert active_workflows[0].target == "metadata_generation"
    assert active_workflows[0].dataset.identifier == "test_dataset_id"

    # New workflow can not be started even when the previous workflow is
    # paused
    workflow.pause()
    with pytest.raises(WorkflowExistsError):
        workflow.preserve()



@pytest.mark.parametrize(
    ("method_name", "arguments"),
    [
        ("generate_metadata", ()),
        ("preserve", ()),
        ("confirm", ()),
        ("reset", ("foo", "bar")),
        ("propose", ()),
        ("reject", ()),
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
        ({}, ["ds1", "ds2", "ds3", "ds4"]),
        # Only active datasets
        ({"active": True}, ["ds1", "ds2"]),
        # Only paused datasets
        ({"paused": True}, ["ds2", "ds4"]),
        # Datasets that are active but paused
        ({"active": True, "paused": True}, ["ds2"]),
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
    for dataset_id in ["ds1", "ds2", "ds3", "ds4"]:
        metadata = copy.deepcopy(DATASET)
        metadata["id"] = dataset_id
        add_metax_dataset(requests_mock=requests_mock, dataset=metadata)

    # Add some workflows to database
    # active, not paused
    workflow1 = Workflow("ds1", config=config)
    workflow1.generate_metadata()
    # active, paused
    workflow2 = Workflow("ds2", config=config)
    workflow2.generate_metadata()
    workflow2.pause()
    # inactive, not paused
    workflow3 = Workflow("ds3", config=config)
    workflow3.generate_metadata()
    workflow3.dataset.add_error("fake error")
    # inactive, paused
    workflow4 = Workflow("ds4", config=config)
    workflow4.generate_metadata()
    workflow4.pause()
    workflow4.dataset.add_error("fake error")

    # Check that find_workflows finds expceted workflows
    assert [
        workflow.dataset.identifier
        for workflow
        in find_workflows(**kwargs, config=config)
    ] == expected_datasets
