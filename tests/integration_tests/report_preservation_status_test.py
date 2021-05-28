"""Integration tests for digital preservation server and
:mod:`siptools_research.workflow.report_preservation_status` module"""

import time

import pytest
from metax_access import Metax
from siptools_research.exceptions import InvalidDatasetError
from siptools_research.workflow import report_preservation_status

from tests.metax_data import datasets

METAX_PATH = "tests/requests_mock_data/metax"


@pytest.fixture(autouse=True)
def mock_metax_access(monkeypatch):
    """Mock metax_access GET requests to files or datasets to return
    mock functions from metax_data.datasets and metax_data.files modules.
    """
    monkeypatch.setattr(
        Metax, "set_preservation_state",
        lambda self, dataset_id, **kwargs: datasets.get_dataset("", dataset_id)
    )


@pytest.mark.usefixtures('testmongoclient')
def test_reportpreservationstatus(testpath, luigi_mock_ssh_config, sftp_dir):
    """Creates new directory to "accepted" directory in digital preservation
    server, runs ReportPreservationStatus task, and tests that task is complete
    after it has been run. Fake Metax server is used, so it can not be tested
    if preservation status really is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Create directory with name of the workspace to digital preservation
    # server, so that the ReportPreservationStatus thinks that
    # validation has completed.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"
    (sftp_dir / "accepted" / datedir / tar_name).mkdir(parents=True)

    # Init and run task
    task = report_preservation_status.ReportPreservationStatus(
        workspace=str(workspace),
        dataset_id="report_preservation_status_test_dataset_ok",
        config=luigi_mock_ssh_config
    )
    assert not task.complete()
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected(
        testpath, luigi_mock_ssh_config, sftp_dir):
    """Creates new directory with a report file to "rejected" directory in
    digital preservation server. Runs ReportPreservationStatus task, which
    should raise an exception and write ingest report HTML to workspace. Fake
    Metax server is used, so it can not be tested if preservation status really
    is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Create directory with name of the workspace to digital preservation
    # server over SSH, so that the ReportPreservationStatus thinks that
    # validation has been rejected.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"
    dir_path = sftp_dir / "rejected" / datedir / tar_name
    dir_path.mkdir(parents=True)

    (dir_path / f"{workspace.name}.html").write_bytes(b"Failed")

    # Init task
    task = report_preservation_status.ReportPreservationStatus(
        workspace=str(workspace),
        dataset_id="report_preservation_status_test_dataset_rejected",
        config=luigi_mock_ssh_config
    )

    # Running task should raise exception
    with pytest.raises(InvalidDatasetError) as exc_info:
        task.run()
    assert str(exc_info.value) == "SIP was rejected"

    # The task should not be completed
    assert not task.complete()


@pytest.mark.usefixtures('testmongoclient')
# pylint: disable=invalid-name
def test_reportpreservationstatus_rejected_int_error(
        testpath, luigi_mock_ssh_config, sftp_dir):
    """Creates new directory to "rejected" and "accepted" directory with two
    report files each in digital preservation server, runs
    ReportPreservationStatus task, and tests that the report file is NOT sent.
    Metax server is used, so it can not be tested if preservation status
    really is updated in Metax.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """

    workspace = testpath

    # Create directory with name of the workspace to digital preservation
    # server over SSH, so that the ReportPreservationStatus thinks that
    # validation has been rejected.
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"

    accepted_report_path = \
        sftp_dir / "accepted" / datedir / tar_name / f"{workspace.name}.html"
    rejected_report_path = \
        sftp_dir / "rejected" / datedir / tar_name / f"{workspace.name}.html"

    accepted_report_path.parent.mkdir(parents=True)
    accepted_report_path.write_bytes(b"Accepted")

    rejected_report_path.parent.mkdir(parents=True)
    rejected_report_path.write_bytes(b"Rejected")

    # Run task like it would be run from command line
    task = report_preservation_status.ReportPreservationStatus(
        workspace=str(workspace),
        dataset_id="report_preservation_status_test_dataset_rejected",
        config=luigi_mock_ssh_config
    )
    assert not task.complete()

    with pytest.raises(ValueError) as exc:
        task.run()

    assert str(exc.value) == "Expected 1 ingest report, found 2"
