"""Test the `siptools_research.transfer.transfer` module"""

import os
import time
from siptools_research.transfer.transfer import MoveTransferToWorkspace
from siptools_research.transfer.transfer import ReadyForTransfer


def test_movetransfertoworkspace(testpath):
    """Test `MoveTransferToWorkspace` task.

    :testpath: Testpath fixture
    :returns: None
    """
    print testpath

    # Create file in temporary directory
    testfilename = "aineisto"
    testfilepath = os.path.join(testpath, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')
    assert os.path.isfile(testfilepath)

    # Create workspace in temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    assert len(os.listdir(os.path.join(workspace))) == 0

    # Move testfile from transfer to workspace
    task = MoveTransferToWorkspace(
        filename=testfilepath,
        workspace_root=workspace,
        min_age=0,
        username='testuser')

    assert not task.complete()
    task.run()
    assert task.complete()

    assert not os.path.isfile(testfilepath)
    assert len(os.listdir(os.path.join(workspace))) == 1

def test_readyfortransfer(testpath):
    """Test `ReadyForTransfer` task

    :testpath: Testpath fixture
    :returns: None
    """

    # Create file in temporary directory
    testfilename = "aineisto"
    testfilepath = os.path.join(testpath, testfilename)
    with open(testfilepath, 'w') as testfile:
        testfile.write('1')

    # ReadyForTransfer.complete should return `False` if called right after
    # creating the file. After waiting for `min_age` seconds it should return
    # `True`.
    task = ReadyForTransfer(
        filename=testfilepath,
        min_age=1,
    )
    assert not task.complete()
    time.sleep(1)
    assert task.complete()
