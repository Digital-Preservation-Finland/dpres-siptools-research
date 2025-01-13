"""Tests for :mod:`siptools_research.workflow.compress` module."""
import shutil
import tarfile

import tests.conftest
from siptools_research.workflow.compress import CompressSIP


def test_compresssip(workspace, tmp_path):
    """Test CompresSIP task.

    Run function should create a tar-file and complete function should
    return ``True`` when tar-file exists.

    :param workspace: Temporary workspace directory
    :param tmp_path: Temporary directory
    """
    (workspace / 'metadata_generation' / 'dataset_files').mkdir(parents=True)

    # Create required contents to workspace
    shutil.copy('tests/data/testsips/simple_sip/mets.xml',
                workspace / 'preservation' / 'mets.xml')
    shutil.copy('tests/data/testsips/simple_sip/signature.sig',
                workspace / 'preservation' / 'signature.sig')
    shutil.copytree('tests/data/testsips/simple_sip/tests',
                    workspace / 'dataset_files' / 'tests')

    # Init task
    task = CompressSIP(dataset_id=workspace.name,
                       config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Extract tar file created by task and that it contains same
    # files/dirs as the original sip-in-progress directory
    with tarfile.open(
        workspace / "preservation" / f"{workspace.name}.tar"
    ) as tar:
        tar.extractall(tmp_path / 'extracted_tar')

    found_files = {
        path.name for path in (tmp_path / "extracted_tar").iterdir()
    }
    assert found_files == {'signature.sig', 'mets.xml', 'dataset_files'}
