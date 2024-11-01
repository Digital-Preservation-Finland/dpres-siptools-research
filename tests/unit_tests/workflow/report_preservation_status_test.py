"""Unit tests for ReportPreservationStatus task."""

import time

import pytest
from siptools_research.exceptions import InvalidDatasetError
from siptools_research.workflow import report_preservation_status
import tests


@pytest.mark.parametrize(
    'dataset_metadata,patched_dataset_id',
    [
        # Ida dataset has been copied to PAS catalog. The preservation
        # state of the dataset in PAS catalog should be updated.
        (
            {
                'identifier': 'original-version-id',
                'preservation_dataset_version': {
                    'identifier': 'pas-version-id'
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
            "pas-version-id"
        ),
        # Pottumounttu dataset is originally created in PAS catalog, so
        # only one version of the dataset exists.
        (
            {
                'identifier': "original-version-id",
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
            'original-version-id'
        )
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_reportpreservationstatus(workspace, requests_mock, dataset_metadata,
                                  patched_dataset_id):
    """Test reporting status of accepted SIP.

    Creates new directory to "accepted" directory in digital
    preservation server and runs ReportPreservationStatus task. Tests
    that task is complete after it has been run and that the
    preservation status of correct dataset is updated.

    :param workspace: Temporary directory fixture
    :param luigi_mock_ssh_config: Luigi configuration file path
    :param sftp_dir: Directory that acts as DPS sftp home directory
    :param requests_mock: HTTP request mocker
    :param dataset_metadata: Dataset metadata in Metax
    :param patched_dataset_id: Dataset identifier that should be used
                               when preservation state is updated.
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json=dataset_metadata
    )
    metax_mock = requests_mock.patch(f'/rest/v2/datasets/{patched_dataset_id}')

    # Create directory with name of the workspace to digital
    # preservation server, so that the ReportPreservationStatus thinks
    # that validation has completed.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"
    (workspace / "validation" / "ingest-reports" / "accepted" / datedir / tar_name).mkdir(parents=True)

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.called_once


@pytest.mark.usefixtures('testmongoclient')
def test_reportpreservationstatus_rejected(workspace, requests_mock):
    """Test reporting status of rejected SIP.

    Creates new directory with a report file to "rejected" directory in
    digital preservation server. Runs ReportPreservationStatus task,
    which should raise an exception and write ingest report HTML to
    workspace.

    :param testpath: Temporary directory fixture
    :param luigi_mock_ssh_config: Luigi configuration file path
    :param sftp_dir: Directory that acts as DPS sftp home directory
    :returns: ``None``
    """
    # Mock metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {'identifier': 'urn:nbn:fi:att:data-catalog-pas'},
            'identifier': 'foobar'
        }
    )

    # Create directory with name of the workspace to digital
    # preservation server over SSH, so that the ReportPreservationStatus
    # thinks that validation has been rejected.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"
    dir_path = \
        workspace / "validation" / "ingest-reports" / "rejected" / datedir / tar_name / f"{workspace.name}.html"
    dir_path.mkdir(parents=True)

    (dir_path / f"{workspace.name}.html").write_bytes(b"Failed")

    # Init task
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Running task should raise exception
    with pytest.raises(InvalidDatasetError) as exc_info:
        task.run()
    assert str(exc_info.value) == "SIP was rejected"

    # The task should not be completed
    assert not task.complete()


@pytest.mark.usefixtures('testmongoclient')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected_int_error(workspace):
    """Test handling conflicting ingest reports.

    Creates ingest report files to "rejected" and "accepted" directories
    in digital preservation server. Runs ReportPreservationStatus task,
    and tests that the report file is NOT sent.

    :param testpath: Temporary directory fixture
    :param luigi_mock_ssh_config: Luigi configuration file path
    :param sftp_dir: Directory that acts as DPS sftp home directory
    :returns: ``None``
    """
    # Create directory with name of the workspace to digital
    # preservation server over SSH, so that the ReportPreservationStatus
    # thinks that validation has been rejected.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"

    accepted_report_path = \
        workspace / "validation" / "ingest-reports" / "accepted" / datedir / tar_name / f"{workspace.name}.html"
    rejected_report_path = \
        workspace / "validation" / "ingest-reports" / "rejected" / datedir / tar_name / f"{workspace.name}.html"

    accepted_report_path.parent.mkdir(parents=True)
    accepted_report_path.write_bytes(b"Accepted")

    rejected_report_path.parent.mkdir(parents=True)
    rejected_report_path.write_bytes(b"Rejected")

    # Run task like it would be run from command line
    task = report_preservation_status.ReportPreservationStatus(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Running task should raise exception
    with pytest.raises(ValueError) as exc:
        task.run()

    assert str(exc.value) == "Expected 1 ingest report, found 2"
