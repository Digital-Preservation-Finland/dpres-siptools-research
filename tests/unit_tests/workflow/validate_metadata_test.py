"""Tests for ``siptools_research.workflow.validate_metadata`` module."""

import os
import pytest
import tests.conftest
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_validatemetadata(testpath):
    """Test ValidateMetadata class. Run task for dataset that has valid
    metadata.

    :testpath: Temporary directory fixture
    :returns: None
    """

    # Create "logs" directory
    os.mkdir(os.path.join(testpath, 'logs'))

    # Init task
    task = ValidateMetadata(workspace=testpath,
                            dataset_id='validate_metadata_test_dataset',
                            config=tests.conftest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()


@pytest.mark.usefixtures('testmongoclient', 'testmetax')
def test_invalid_metadata(testpath):
    """Test ValidateMetadata class. Run task for dataset that has invalid
    metadata. The dataset is missing attribute: 'type' for each object in files
    list.

    :testpath: Temporary directory fixture
    :returns: None
    """

    # Create "logs" directory
    os.mkdir(os.path.join(testpath, 'logs'))

    # Init task
    task = ValidateMetadata(workspace=testpath,
                            dataset_id='validate_metadata_test_dataset_invalid_metadata',
                            config=tests.conftest.TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "'contract' is a required property" in exc.value[0]
    assert not task.complete()
