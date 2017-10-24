"""Test the ``siptools_research.workflow.get_files`` module"""

import os
from siptools_research.workflow import get_files

# pylint: disable=unused-argument,invalid-name,fixme
def test_getfiles(testpath, testmetax, testida):
    """Tests for ``GetFiles`` task.

    - ``Task.complete()`` is true after ``Task.run()``
    - File is copied to correct path

    :testpath: Testpath fixture
    :returns: None
    """

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="2")
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    with open(os.path.join(testpath,
                           'files/some/path/file_name_1')) as open_file:
        assert open_file.read() == 'foo\n'

    with open(os.path.join(testpath,
                           'files/some/path/file_name_2')) as open_file:
        assert open_file.read() == 'bar\n'
