"""Test the ``siptools_research.workflow.get_files`` module"""

import os
import pytest
import tests.conftest
from siptools_research.workflow import get_files

@pytest.mark.usefixtures('testmongoclient', 'testmetax', 'testida')
def test_getfiles(testpath):
    """Tests for ``GetFiles`` task.

    - ``Task.complete()`` is true after ``Task.run()``
    - Files are copied to correct path

    :testpath: Testpath fixture
    :returns: None
    """

    # Create required directories to  workspace
    os.makedirs(os.path.join(testpath, 'sip-in-progress'))
    os.makedirs(os.path.join(testpath, 'logs'))


    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset",
                              config=tests.conftest.TEST_CONFIG_FILE)
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


@pytest.mark.usefixtures('testmetax', 'testida')
def test_missing_files(testpath):
    """Test case where a file can not be found from Ida. The first file should
    successfully downloaded, but the second file is not found in Ida. Task
    should fail with Exception.

    :testpath: Temporary directory fixture
    """

    # Init task
    task = get_files.GetFiles(workspace=testpath,
                              dataset_id="get_files_test_dataset_ida_missing_file",
                              config=tests.conftest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    with pytest.raises(Exception) as excinfo:
        task.run()

    # Check exception message
    assert str(excinfo.value) == \
        "File pid:urn:does_not_exist not found in Ida."

    # Task should not be completed
    assert not task.complete()

    # The first file should be created into correct path
    with open(os.path.join(testpath, 'sip-in-progress/project_x_FROZEN/'\
                           'Experiment_X/file_name_3'))\
            as open_file:
        assert open_file.read() == 'foo\n'
