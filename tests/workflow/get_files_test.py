"""Test the ``siptools_research.workflow.get_files`` module"""

import os
from siptools_research.workflow import get_files

# pylint: disable=unused-argument,invalid-name,fixme
def test_getfiles(testpath):
    """Tests for ``GetFiles`` task.

    - ``Task.complete()`` is true after ``Task.run()``
    - File is copied to correct path

    :testpath: Testpath fixture
    :returns: None
    """

    # Create empty workspace
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)

    # Init task
    task = get_files.GetFiles(workspace=workspace,
                              dataset_id=1)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct file is created into correct path
    with open(os.path.join(workspace,
                           'files/some/path/file_name_1')) as open_file:
        assert open_file.read() == 'foo\n'
