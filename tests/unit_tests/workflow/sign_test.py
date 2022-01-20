"""Test the :mod:`siptools_research.workflow.sign` module."""

import shutil
import tests.conftest
from siptools_research.workflow import sign


def test_signsip(workspace):
    """Tests for `SignSIP` task.

    - `Task.complete()` is true after `Task.run()`
    - Signature file created
    - Log file is created

    :param workspace: Test workspace directory fixture
    :returns: ``None``
    """
    # Copy sample METS file to workspace
    shutil.copy(
        'tests/data/sample_mets.xml',
        workspace / 'mets.xml'
    )

    # Init task
    task = sign.SignSIP(workspace=str(workspace),
                        dataset_id="1",
                        config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that signature.sig is created in workspace
    assert (
        "This is an S/MIME signed message"
        in (workspace / "signature.sig").read_text()
    )

    names = set(path.name for path in workspace.iterdir())

    # SIP directory should contain only METS and signature
    assert names == set(['signature.sig', 'mets.xml'])
