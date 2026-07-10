"""Unit tests for ReportPreservationStatus task."""
import copy
import re

import pytest

from siptools_research.exceptions import InvalidDatasetError
from siptools_research.tasks import report_preservation_status
from metax_access.template_data import DATASET


PRESERVATION = DATASET["preservation"]


@pytest.mark.parametrize(
    ("dataset_metadata", "preserved_dataset_ids", "patched_dataset_id"),
    [
        # Ida dataset has been copied to PAS catalog. Both versions
        # should be marked preserved, and the preservation state of the
        # dataset in PAS catalog should be updated.
        (
            {
                "id": "original-version-id",
                "preservation": PRESERVATION | {
                    "state": 0,
                    "dataset_version": {
                        "id": "pas-version-id",
                        "persistent_identifier": None,
                        "preservation_state": 0
                    },
                },
            },
            ["original-version-id", "pas-version-id"],
            "pas-version-id"
        ),
        # Pottumounttu dataset is originally created in PAS catalog, so
        # only one version of the dataset exists.
        (
            {
                "id": "original-version-id",
                "preservation": PRESERVATION | {
                    "state": 0,
                    "dataset_version": {
                        "id": None,
                        "persistent_identifier": None,
                        "preservation_state": 0
                    },
                },
            },
            ["original-version-id"],
            'original-version-id'
        )
    ]
)
def test_reportpreservationstatus(config, workspace, requests_mock,
                                  dataset_metadata, preserved_dataset_ids,
                                  patched_dataset_id):
    """Test reporting status of accepted SIP.

    Creates the GetSIPStatus output file with "accepted"
    status, and runs ReportPreservationStatus task. Tests that task is
    complete after it has been run and that the preservation status of
    correct dataset is updated.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :param dataset_metadata: Dataset metadata in Metax
    :param preserved_dataset_ids: Dataset identifiers that should be
                                  marked preserved.
    :param patched_dataset_id: Dataset identifier that should be used
                               when preservation state is updated.
    """
    # Mock Metax
    dataset = copy.deepcopy(DATASET)
    dataset.update(dataset_metadata)
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    requests_mock.get(f"/v3/datasets/{workspace.name}/files",
                      json={"next": None, "results": []})
    # This is required because unlocking the dataset currently
    # unlocks also all files of the dataset. In future, Metax might
    # unlock the files automatically.
    requests_mock.post("/v3/files/patch-many")
    matcher = re.compile("/v3/datasets/.*/preservation")
    metax_mock = requests_mock.patch(matcher, json={})

    # Create the GetSIPStatus output file, so that the
    # ReportPreservationStatus thinks that validation has completed.
    validation_status = (
        workspace / "preservation" / "task-get-validation-reports.finished"
    )
    validation_status.write_text('accepted')

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=config,
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert len(metax_mock.request_history) == len(preserved_dataset_ids) + 2
    # Every version of the dataset should be marked preserved
    for request, dataset_id in zip(metax_mock.request_history,
                                   preserved_dataset_ids):
        assert dataset_id in request.url
        assert request.json() == {
            "pas_package_created": True
        }
    # The original dataset should be unlocked
    assert workspace.name in metax_mock.request_history[-2].url
    assert metax_mock.request_history[-2].json() == {
        "pas_process_running": False
    }
    # The preservation state should be updated
    assert patched_dataset_id in metax_mock.request_history[-1].url
    assert metax_mock.request_history[-1].json() == {
        "state": 120,
        "description": {"en": "In digital preservation"}
    }


def test_reportpreservationstatus_rejected(config, workspace):
    """Test reporting status of rejected SIP.

    Creates the GetSIPStatus output file with "rejected"
    status. Runs ReportPreservationStatus task, which should raise an
    exception.

    :param config: Configuration file
    :param workspace: Temporary worpspace directory
    :param requests_mock: HTTP request mocker
    """
    validation_status = (
        workspace / "preservation" / "task-get-validation-reports.finished"
    )
    validation_status.write_text('rejected')

    # Init task
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=config,
    )

    # Running task should raise exception
    with pytest.raises(InvalidDatasetError) as exc_info:
        task.run()
    assert str(exc_info.value) == "SIP was rejected"

    # The task should not be completed
    assert not task.complete()


# pylint: disable=invalid-name
def test_reportpreservationstatus_unexpected_status(config, workspace):
    """Test handling an unexpected transfer status.

    Creates the GetSIPStatus output file with an unexpected
    status. Runs ReportPreservationStatus task, and tests that the
    report file is NOT sent.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    """
    validation_status = (
        workspace / "preservation" / "task-get-validation-reports.finished"
    )
    validation_status.write_text('unknown')

    # Run task like it would be run from command line
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()

    # Running task should raise exception
    with pytest.raises(
            ValueError, match="Unexpected transfer status: unknown"
    ):
        task.run()
