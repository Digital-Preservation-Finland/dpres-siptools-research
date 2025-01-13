"""Unit tests for ReportPreservationStatus task."""

import pytest

import tests
from siptools_research.exceptions import InvalidDatasetError
from siptools_research.workflow import report_preservation_status


@pytest.mark.parametrize(
    ("dataset_metadata", "patched_dataset_id"),
    [
        # Ida dataset has been copied to PAS catalog. The preservation
        # state of the dataset in PAS catalog should be updated.
        (
            {
                'identifier': 'original-version-id',
                'preservation_dataset_version': {
                    'identifier': 'pas-version-id'
                },
                "preservation_identifier": "doi:test",
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
                "preservation_identifier": "doi:test",
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

    Creates new ''ingest-reports/accepted'' directory in local workspace
    and runs ReportPreservationStatus task. Tests
    that task is complete after it has been run and that the
    preservation status of correct dataset is updated.

    :param workspace: Temporary directory fixture
    :param luigi_mock_ssh_config: Luigi configuration file path
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

    # Create directory with name of the workspace to the local
    # workspace directory, so that the ReportPreservationStatus thinks
    # that validation has completed.
    ingest_report = (workspace / "preservation" / "ingest-reports" / "accepted"
                     / f"{dataset_metadata['preservation_identifier']}.xml")
    ingest_report.parent.mkdir(parents=True, exist_ok=True)
    ingest_report.write_text('foo')

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

    Creates new directory with a report file to "rejected" directory.
    Runs ReportPreservationStatus task,
    which should raise an exception.

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
            'identifier': 'foobar',
            "preservation_identifier": "doi:test"
        }
    )

    ingest_report = (workspace / "preservation" / "ingest-reports" / "rejected"
                     / "doi:test.xml")
    ingest_report.parent.mkdir(parents=True, exist_ok=True)
    ingest_report.write_text('foo')

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

    Creates ingest report files to "rejected" and "accepted"
    directories. Runs ReportPreservationStatus task, and tests that the
    report file is NOT sent.

    :param testpath: Temporary directory fixture
    :param luigi_mock_ssh_config: Luigi configuration file path
    :param sftp_dir: Directory that acts as DPS sftp home directory
    :returns: ``None``
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
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Running task should raise exception
    with pytest.raises(ValueError) as exc:
        task.run()

    assert str(exc.value) == "Expected 1 ingest report, found 2"
