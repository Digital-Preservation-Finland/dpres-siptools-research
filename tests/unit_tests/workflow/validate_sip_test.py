"""Unit tests for :mod:`siptools_research.workflow.validate_sip`."""
import os
import time

import pytest

from siptools_research.workflow.validate_sip import ValidateSIP


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_accepted(workspace, luigi_mock_ssh_config, sftp_dir):
    """Initializes validate_sip task and tests that it is not complete. Then
    creates new directory to "accepted" directory in digital preservation
    server and tests that task is complete.

    :param workspace: Temporary directory fixture
    :returns: ``None``
    """
    # Init task
    task = ValidateSIP(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    (sftp_dir / "accepted" / datedir / tar_name).mkdir(
        parents=True, exist_ok=True
    )

    # Check that task is completed after new directory is created
    assert not task.complete()
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_rejected(workspace, luigi_mock_ssh_config, sftp_dir):
    """Initializes validate-sip task and tests that it is not complete. Then
    creates new directory to "rejected" directory in digital preservation
    server and tests that task is complete.

    :param workspace: Temporary directory fixture
    :returns: ``None``
    """
    # Init task
    task = ValidateSIP(dataset_id=workspace.name, config=luigi_mock_ssh_config)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = f"{workspace.name}.tar"
    (sftp_dir / "rejected" / datedir / tar_name).mkdir(
        parents=True, exist_ok=True
    )

    # Check that task is completed after new directory is created
    assert not task.complete()
    task.run()
    assert task.complete()
