"""Tests for :mod:`siptools_research.workflow.compress` module."""
import os
import shutil
import tarfile
import tests.conftest
from siptools_research.workflow.compress import CompressSIP


def test_compresssip(testpath):
    """Test CompresSIP task.

    Run function should create a tar-file and complete function should
    return ``True`` when tar-file exists.

    :param testpath: Temporary directory fixture
    """
    workspace = os.path.join(testpath, 'workspaces', 'workspace')
    os.makedirs(os.path.join(workspace, 'dataset_files'))
    # Create required contents to workspace
    shutil.copy('tests/data/testsips/simple_sip/mets.xml',
                os.path.join(workspace, 'mets.xml'))
    shutil.copy('tests/data/testsips/simple_sip/signature.sig',
                os.path.join(workspace, 'signature.sig'))
    shutil.copytree('tests/data/testsips/simple_sip/tests',
                    os.path.join(workspace, 'dataset_files', 'tests'))

    # Init task
    task = CompressSIP(workspace=workspace,
                       dataset_id="1",
                       config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Extract tar file created by task and that it contains same
    # files/dirs as the original sip-in-progress directory
    with tarfile.open(
            os.path.join(workspace, os.path.basename(workspace)) + '.tar'
    ) as tar:
        tar.extractall(os.path.join(testpath, 'extracted_tar'))

    assert set(os.listdir(os.path.join(testpath, 'extracted_tar'))) \
        == set(['signature.sig', 'mets.xml', 'dataset_files'])
