"""Test the :mod:`siptools_research.workflow.sign` module."""

import shutil

from siptools_research.workflow import sign


def test_signsip(config, workspace):
    """Tests for `SignSIP` task.

    - `Task.complete()` is true after `Task.run()`
    - Signature file created
    - Log file is created

    :param config: Configuration file
    :param workspace: Test workspace directory fixture
    """
    # Copy sample METS file to workspace
    shutil.copy(
        'tests/data/sample_mets.xml',
        workspace / 'preservation' / 'mets.xml'
    )

    # Init task
    task = sign.SignSIP(dataset_id=workspace.name, config=config)
    assert not task.complete()

    # Run task.
    task.run()
    assert task.complete()

    # Check that signature.sig is created in workspace
    assert (
        "This is an S/MIME signed message"
        in (workspace / "preservation" / "signature.sig").read_text()
    )

    names = {path.name for path in (workspace / "preservation").iterdir()}

    # Preservation workspace should contain only METS, signature, and
    # sip-in-progress directory.
    assert names == {'signature.sig', 'mets.xml'}
