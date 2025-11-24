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


def test_validate_dataset(config, requests_mock):
    """Test validation.

    Tests that `validate` method:

    * sets correct target for the workflow
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

    # Metadata must be confirmed before validation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

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


def test_restart_validate(config, requests_mock):
    """Test restarting validation.

    When validation is restarted, previous validation and preservation
    workspaces should be cleared.

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


def test_reset(requests_mock, config):
    """Test resetting workflow.

    Tests that dataset metadata in Metax is updated and dataset is
    unlocked.

    :param requests_mock: HTTP request mocker
    :param config: Configuration file
    """
    # Mock Metax
    add_metax_dataset(requests_mock)

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
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.enabled = True
    workflow_entry.target = "metadata_generation"
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    # Try to start another workflow
    workflow = Workflow("test_dataset_id", config=config)
    with pytest.raises(WorkflowExistsError):
        workflow.preserve()

    # New workflows should not be created and the existing workflow
    # should not be changed
    active_workflows = find_workflows(enabled=True, config=config)
    assert len(active_workflows) == 1
    assert active_workflows[0].target.value == "metadata_generation"
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
    ("method_name", "arguments"),
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
    workflowentry3 = WorkflowEntry(id="ds3")
    workflowentry3.metadata_confirmed = True
    workflowentry3.save()
    workflow3 = Workflow('ds3', config=config)
    workflow3.validate()
    workflowentry4 = WorkflowEntry(id="ds4")
    workflowentry4.metadata_confirmed = True
    workflowentry4.save()
    workflow4 = Workflow('ds4', config=config)
    workflow4.validate()
    workflow4.disable()
    workflowentry5 = WorkflowEntry(id="ds5")
    workflowentry5.metadata_confirmed = True
    workflowentry5.save()
    workflow5 = Workflow('ds5', config=config)
    workflow5.preserve()
    workflowentry6 = WorkflowEntry(id="ds6")
    workflowentry6.metadata_confirmed = True
    workflowentry6.save()
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
