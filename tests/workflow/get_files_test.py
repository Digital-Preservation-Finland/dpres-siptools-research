"""Test the ``siptools_research.workflow.get_files`` module"""

import os
import pytest
from siptools_research.workflow import get_files

# pylint: disable=unused-argument
def test_getfiles(testpath, testmetax, testida, testmongoclient):
    """Tests for ``GetFiles`` task.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :testpath: Testpath fixture
    :returns: None
    """

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset_1",
                              config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that correct files are created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/project_x_FROZEN/'\
                           'Experiment_X/file_name_3'))\
            as open_file:
        assert open_file.read() == 'foo\n'

    with open(os.path.join(testpath, 'sip-in-progress/project_x_FROZEN/'\
                           'Experiment_X/file_name_4'))\
            as open_file:
        assert open_file.read() == 'bar\n'

    # Check that  logical structmap is created
    with open(os.path.join(testpath,
                           'sip-in-progress/logical_struct')) as open_file:
        assert open_file.read() == '{"Source material": ["/project_x_FROZEN/'\
                                   'Experiment_X/file_name_3", "'\
                                   '/project_x_FROZEN/Experiment_X/'\
                                   'file_name_4"]}'


def test_missing_files(testpath, testmetax, testida):
    """Test case where a file can not be found from Ida. The first file should
    successfully downloaded, but the second file is not found in Ida. Task
    should fail with Exception."""

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset_2",
                              config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) == "File not found in Ida."

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/project_x_FROZEN/'\
                           'Experiment_X/file_name_3'))\
            as open_file:
        assert open_file.read() == 'foo\n'
