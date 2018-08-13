"""Tests for ``siptools_research.workflow.compress`` module"""
import os
import shutil
import tarfile
import tests.conftest
from siptools_research.workflow.compress import CompressSIP

def test_compresssip(testpath):
    """Test that run-function of CompresSIP task creates tar-file
    complete-function returns True when tar-file exists.

    :testpath: Temporary directory fixture"""
    # Create required contents to workspace
    os.makedirs(os.path.join(testpath, 'logs'))
    shutil.copytree('tests/data/testsips/simple_sip',
                    os.path.join(testpath, 'sip-in-progress'))

    # Init task
    task = CompressSIP(workspace=testpath,
                       dataset_id="1",
                       config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Extract tar file created by task and that it contains same files/dirs as
    # the original sip-in-progress directory
    with tarfile.open(os.path.join(testpath, os.path.basename(testpath)) \
                      + '.tar') as tar:
        tar.extractall(os.path.join(testpath, 'extracted_tar'))
    assert set(os.listdir(os.path.join(testpath, 'extracted_tar'))) \
        == set(['signature.sig', 'mets.xml', 'tests'])
