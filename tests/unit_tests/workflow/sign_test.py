"""Test the `siptools_research.workflow.sign` module"""

import os
import shutil
import pytest
import tests.conftest
from siptools_research.workflow import sign


@pytest.mark.usefixtures('testmetax')
def test_signsip(testpath):
    """Tests for `SignSIP` task.

    - `Task.complete()` is true after `Task.run()`
    - Signature file created
    - Log file is created

    :param testpath: Testpath fixture
    :returns: ``None``
    """

    # Create workspace with "logs" and "transfers" directories in temporary
    # directory
    workspace = testpath
    os.makedirs(os.path.join(workspace, 'logs'))
    sip_path = os.path.join(workspace, 'sip-in-progress')
    os.makedirs(sip_path)

    # Copy sample METS file to workspace
    shutil.copy(
        'tests/data/sample_mets.xml', os.path.join(sip_path, 'mets.xml')
    )

    # Init task
    task = sign.SignSIP(workspace=workspace,
                        dataset_id="1",
                        config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that signature.sig is created in workspace/sip-in-progress/
    with open(os.path.join(workspace,
                           'sip-in-progress',
                           'signature.sig'))\
            as open_file:
        assert "This is an S/MIME signed message" in open_file.read()

    # Check that log is created in workspace/logs/
    with open(os.path.join(workspace,
                           'logs',
                           'task-sign-sip.log'))\
            as open_file:
        assert open_file.read().startswith("sign_mets created file")
