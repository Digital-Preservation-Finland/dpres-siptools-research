"""Tests for ``siptools_research.api.dataset`` module."""
import copy

from metax_access import (
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_REJECTED_BY_USER,
)
from metax_access.template_data import DATASET

import tests.utils
from siptools_research.models.workflow_entry import WorkflowEntry
from siptools_research.workflow import Workflow


def test_dataset_status(client, requests_mock, mocker):
    """Test checking dataset status.

    :param client: Flask test client
    :param requests_mock: HTTP request mocker
    :param mocker: MockerFixture object
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    # Spy the methods that should be called.
    require_methods_to_be_called = [
        mocker.spy(Workflow, method)
        for method in (
            "require_generate_metadata_allowed",
            "require_confirm_allowed",
            "require_propose_allowed",
            "require_preserve_allowed",
            "require_reset_allowed",
            "require_reject_allowed",
        )
    ]

    response = client.get("/dataset/test_dataset_id/status")
    assert response.status_code == 200
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "paused": False,
        "target": None,
        "active": False,
        "metadata_confirmed": False,
        "proposed": False,
        "errors": [],
        "actions": {
            "reset": {
                "allowed": True,
                "reason": None,
                "url": "/dataset/test_dataset_id/reset",
            },
            "generate_metadata": {
                "allowed": True,
                "reason": None,
                "url": "/dataset/test_dataset_id/generate-metadata",
            },
            "confirm": {
                "allowed": False,
                "reason": "Metadata has not been generated.",
                "url": "/dataset/test_dataset_id/confirm",
            },
            "propose": {
                "allowed": False,
                "reason": "Metadata has not been confirmed.",
                "url": "/dataset/test_dataset_id/propose",
            },
            "preserve": {
                "allowed": False,
                "reason": "Metadata has not been confirmed.",
                "url": "/dataset/test_dataset_id/preserve",
            },
            "reject": {
                "allowed": False,
                "reason": "Dataset has not been proposed for preservation.",
                "url": "/dataset/test_dataset_id/reject",
            },
        }
    }

    # It would be laborious to test that preconditions are correctly
    # checked for each action, but at least we can check that all
    # require_* methods of Workflow class are called once.
    assert all(
        method.call_count == 1
        for method in require_methods_to_be_called
    )


def test_dataset_preserve(client, config, requests_mock):
    """Test preserving dataset.

    :param client: Flask test client
    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    # Metadata must be confirmed before preservation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    response = client.post("/dataset/test_dataset_id/preserve")
    assert response.status_code == 202
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "status": "preserving"
    }

    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.target == "preservation"


def test_dataset_generate_metadata(client, config, requests_mock):
    """Test the generating metadata.

    :param client: Flask test client
    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    response = client.post("/dataset/test_dataset_id/generate-metadata")
    assert response.status_code == 202
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "status": "generating metadata"
    }

    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.target == "metadata_generation"


def test_dataset_confirm(client, requests_mock, workspace):
    """Test confirming dataset.

    :param client: Flask test client
    :param requests_mock: HTTP Request mocker
    :param workspace: Temporary workspace directory
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset["id"] = workspace.name
    tests.utils.add_metax_dataset(requests_mock, dataset=dataset)
    preservation_patch = requests_mock.patch(
        f"/v3/datasets/{workspace.name}/preservation", json={}
    )

    # Add a fake output for metadata generation workflow, so it looks
    # like metadata would be generated
    output_path = (workspace
     / "metadata_generation"
     / "generate-metadata.finished")
    output_path.parent.mkdir()
    output_path.write_text("Fake output")

    response = client.post(f"/dataset/{workspace.name}/confirm")
    assert response.status_code == 200
    assert response.json == {
        "dataset_id": workspace.name,
        "status": "dataset metadata has been confirmed"
    }

    assert preservation_patch.last_request.json() == {
        "state": DS_STATE_METADATA_CONFIRMED,
        "description": {"en": "Metadata confirmed by user"}
    }


def test_dataset_reset(client, requests_mock):
    """Test resetting the dataset.

    :param client: Flask test client
    :param requests_mock: HTTP Request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)
    patch_many = requests_mock.post("/v3/files/patch-many")

    response = client.post(
        "/dataset/test_dataset_id/reset",
        data={
            "description": "Reset by user",
            "reason_description": "File was incorrect"
        }
    )
    assert response.status_code == 200
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "status": "dataset has been reset"
    }

    # Dataset does not contain any files, so empty list of files should
    # be patched
    assert patch_many.called_once
    assert patch_many.last_request.json() == []


def test_propose_dataset(client, config, requests_mock):
    """Test proposing dataset for preservation.

    When dataset is proposed for preservation, the validation workflow
    should be initialized.

    :param client: Flask test client
    :param config: Configuration file
    :param requests_mock: HTTP Request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)

    # Metadata must be confirmed before validation
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.metadata_confirmed = True
    workflow_entry.save()

    response = client.post("/dataset/test_dataset_id/propose")
    assert response.status_code == 202
    assert response.json == {"dataset_id": "test_dataset_id",
                             "status": "validating dataset"}

    workflow = Workflow("test_dataset_id", config=config)
    assert workflow.proposed is True
    assert workflow.target == "validation"


def test_dataset_reject(client, requests_mock):
    """Test rejecting the dataset.

    :param client: Flask test client
    :param requests_mock: HTTP Request mocker
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock)
    preservation_patch = requests_mock.patch(
        "/v3/datasets/test_dataset_id/preservation", json={}
    )

    # Dataset must be proposed for preservation before it can be
    # rejected
    workflow_entry = WorkflowEntry(id="test_dataset_id")
    workflow_entry.proposed = True
    workflow_entry.save()

    response = client.post(
        "/dataset/test_dataset_id/reject",
    )
    assert response.status_code == 200
    assert response.json == {
        "dataset_id": "test_dataset_id",
        "status": "dataset has been rejected"
    }

    assert preservation_patch.called_once
    assert preservation_patch.last_request.json() == {
        "state": DS_STATE_REJECTED_BY_USER,
        "description": {"en": "Rejected by user"}
    }
