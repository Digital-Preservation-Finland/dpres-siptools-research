"""Unit tests for ReportPreservationStatus task."""
import copy
import re

import pytest

from siptools_research.exceptions import InvalidDatasetError
from siptools_research.workflow import report_preservation_status
from metax_access.template_data import DATASET


@pytest.mark.parametrize(
    ("dataset_metadata", "patched_dataset_id"),
    [
        # Ida dataset has been copied to PAS catalog. The preservation
        # state of the dataset in PAS catalog should be updated.
        (
            {
                "id": "original-version-id",
                "preservation": {
                    "state": 0,
                    "description": None,
                    "reason_description": None,
                    "contract": None,
                    "dataset_version": {
                        "id": "pas-version-id",
                        "persistent_identifier": None,
                        "preservation_state": 0
                    },
                    "pas_package_created": None,
                },
            },
            "pas-version-id"
        ),
        # Pottumounttu dataset is originally created in PAS catalog, so
        # only one version of the dataset exists.
        (
            {
                "id": "original-version-id",
                "preservation": {
                    "state": 0,
                    "description": None,
                    "reason_description": None,
                    "contract": None,
                    "dataset_version": {
                        "id": None,
                        "persistent_identifier": None,
                        "preservation_state": 0
                    },
                    "pas_package_created": None,
                },
            },
            'original-version-id'
        )
    ]
)
def test_reportpreservationstatus(config, workspace, requests_mock,
                                  dataset_metadata, patched_dataset_id):
    """Test reporting status of accepted SIP.

    Creates new ''ingest-reports/accepted'' directory in local workspace
    and runs ReportPreservationStatus task. Tests
    that task is complete after it has been run and that the
    preservation status of correct dataset is updated.

    :param config: Configuration file
    :param workspace: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :param dataset_metadata: Dataset metadata in Metax
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

    # Create directory with name of the workspace to the local
    # workspace directory, so that the ReportPreservationStatus thinks
    # that validation has completed.
    ingest_report = (workspace / "preservation" / "ingest-reports" / "accepted"
                     / "doi:test.xml")
    ingest_report.parent.mkdir(parents=True, exist_ok=True)
    ingest_report.write_text('foo')

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=config,
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert len(metax_mock.request_history) == 3
    # The dataset should be marked preserved
    assert patched_dataset_id in metax_mock.request_history[0].url
    assert metax_mock.request_history[0].json() == {
        "pas_package_created": True
    }
    # The original dataset should be unlocked
    assert workspace.name in metax_mock.request_history[1].url
    assert metax_mock.request_history[1].json() == {
        "pas_process_running": False
    }
    # The preservation state should be updated
    assert patched_dataset_id in metax_mock.request_history[2].url
    assert metax_mock.request_history[2].json() == {
        "state": 120,
        "description": {"en": "In digital preservation"}
    }


def test_reportpreservationstatus_rejected(config, workspace):
    """Test reporting status of rejected SIP.

    Creates new directory with a report file to "rejected" directory.
    Runs ReportPreservationStatus task,
    which should raise an exception.

    :param config: Configuration file
    :param workspace: Temporary worpspace directory
    :param requests_mock: HTTP request mocker
    """
    ingest_report = (workspace / "preservation" / "ingest-reports" / "rejected"
                     / "doi:test.xml")
    ingest_report.parent.mkdir(parents=True, exist_ok=True)
    ingest_report.write_text('foo')

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
def test_reportpreservationstatus_rejected_int_error(config, workspace):
    """Test handling conflicting ingest reports.

    Creates ingest report files to "rejected" and "accepted"
    directories. Runs ReportPreservationStatus task, and tests that the
    report file is NOT sent.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    """
    # Create directory with name of the workspace to digital
    # preservation server over SSH, so that the ReportPreservationStatus
    # thinks that validation has been rejected.

    accepted_report_path = (workspace / "preservation" / "ingest-reports" /
                            "accepted" / "doi:test.xml")
    rejected_report_path = (workspace / "preservation" / "ingest-reports" /
                            "rejected" / "doi:test.xml")

    accepted_report_path.parent.mkdir(parents=True)
    accepted_report_path.write_bytes(b"Accepted")

    rejected_report_path.parent.mkdir(parents=True)
    rejected_report_path.write_bytes(b"Rejected")

    # Run task like it would be run from command line
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()

    # Running task should raise exception
    with pytest.raises(ValueError, match="Expected 1 ingest report, found 2"):
        task.run()
