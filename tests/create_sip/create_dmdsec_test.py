"""Test the `siptools_research.create_sip.create_dmdsec` module"""

import os
import shutil
from siptools_research.create_sip.create_dmdsec\
    import CreateDescriptiveMetadata
DATASET_PATH = "tests/data/metax_datasets/"

def test_createdescriptivemetadata(testpath):
    """Test `CreateDescriptiveMetadata` task.

    :testpath: Testpath fixture
    :returns: None
    """

    # Create workspace with "logs" and "sip-in-progress' directories in
    # temporary directory
    workspace = os.path.join(testpath, 'workspace')
    os.makedirs(workspace)
    os.makedirs(os.path.join(workspace, 'logs'))
    os.makedirs(os.path.join(workspace, 'sip-in-progress'))

    # Copy sample datacite.xml to workspace directory
    shutil.copy('tests/data/datacite_sample.xml',
                os.path.join(workspace, 'sip-in-progress', 'datacite.xml'))

    # Init task
    task = CreateDescriptiveMetadata(home_path=workspace,
                                     workspace=workspace)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()

    # Check that XML is created
    assert os.path.isfile(os.path.join(workspace,
                                       'sip-in-progress',
                                       'dmdsec.xml'))


# TODO: Test for CreateDescriptiveMetadata.requires()

# TODO: Test for ReadyForThis

# TODO: Test for DmdsecComplete
