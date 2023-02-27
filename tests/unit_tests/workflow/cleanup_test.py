"""Tests for :mod:`siptools_research.workflow.cleanup` module."""
import pytest

import tests.conftest
from siptools_research.workflow.cleanup import CleanupFileCache


@pytest.mark.usefixtures("testmongoclient")
def test_cleanupfilecache(workspace, requests_mock, pkg_root):
    """Test that task.run() removes files from file cache.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param pkg_root: Root directory of packaging service
    """
    # Mock metax
    requests_mock.get("https://metaksi/rest/v2/datasets/identifier/files",
                      json=[{'identifier': 'file1'}, {'identifier': 'file2'}])

    # Add dataset files to file cache
    for file_id in ['file1', 'file2']:
        (pkg_root / 'file_cache' / file_id).write_text('foo')

    # Init task
    task = CleanupFileCache(workspace=str(workspace), dataset_id='identifier',
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert not task.complete()
    task.run()
    assert task.complete()

    # The file cache should be empty
    assert not list((pkg_root / 'file_cache').iterdir())
