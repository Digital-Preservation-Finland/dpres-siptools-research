"""Integration tests for digital preservation service and
:mod:`siptools_research.workflow.validate_sip` module.
"""
from __future__ import unicode_literals

import copy
import os
import shutil
import time

import paramiko
import pytest
from siptools_research.workflow.validate_sip import ValidateSIP

import tests.conftest


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_accepted(testpath, luigi_mock_ssh_config, sftp_dir):
    """Initializes validate_sip task and tests that it is not complete. Then
    creates new directory to "accepted" directory in digital preservation
    server and tests that task is complete.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=luigi_mock_ssh_config)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    path = os.path.join(
        str(sftp_dir), "accepted/{}/{}".format(datedir, tar_name)
    )
    _create_sftp_dir(path)

    # Check that task is completed after new directory is created
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_validatesip_rejected(testpath, luigi_mock_ssh_config, sftp_dir):
    """Initializes validate-sip task and tests that it is not complete. Then
    creates new directory to "rejected" directory in digital preservation
    server and tests that task is complete.

    :param testpath: Temporary directory fixture
    :returns: ``None``
    """
    workspace = testpath

    # Init task
    task = ValidateSIP(workspace=workspace, dataset_id="1",
                       config=luigi_mock_ssh_config)
    assert not task.complete()

    # Create new directory to digital preservation server
    datedir = time.strftime("%Y-%m-%d")
    tar_name = os.path.basename(workspace) + '.tar'
    path = os.path.join(
        str(sftp_dir), "rejected/{}/{}".format(datedir, tar_name)
    )
    _create_sftp_dir(path)

    # Check that task is completed after new directory is created
    assert task.complete()


def _create_sftp_dir(path):
    """Creates new directory to digital preservation server and sets
    permissions of the new directory and its parent directory.

    :param path: Path of new directory
    :returns: ``None``
    """
    try:
        os.makedirs(path)
    except OSError:
        # Directory already exists
        pass
