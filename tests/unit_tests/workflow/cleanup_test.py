"""Tests for :mod:`siptools_research.workflow.cleanup` module."""
import pytest

import tests.conftest
from siptools_research.workflow.cleanup import Cleanup


@pytest.mark.usefixtures("testmongoclient")
def test_cleanupfilecache(workspace, requests_mock, pkg_root):
    """Test that task.run() removes files from file cache.

    :param workspace: Temporary workspace directory fixture
    :param requests_mock: Mocker object
    :param pkg_root: Root directory of packaging service
    """
    # Mock metax
    requests_mock.get(
        f"https://metaksi/rest/v2/datasets/{workspace.name}/files",
        json=[
            {'identifier': 'file1'},
            {'identifier': 'file2'}
        ]
    )

    # Add some files to file cache
    for file_id in ['file1', 'file2', 'file3']:
        (pkg_root / 'file_cache' / file_id).write_text('foo')

    # Init task
    task = Cleanup(dataset_id=workspace.name,
                   config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert not task.complete()
    task.run()
    assert task.complete()

    # All files of the dataset should be from cache, so only files of
    # other datasets should be left.
    assert [path.name for path in (pkg_root / 'file_cache').iterdir()] \
        == ['file3']

    # The workspace should be removed
    assert not workspace.exists()
