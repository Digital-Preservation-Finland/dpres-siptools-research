"""Tests for ``siptools_research.workflow.validate_metadata`` module."""

import os
import pytest
from siptools_research.luigi.task import InvalidMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata


def test_validatemetadata(testpath, testmetax, testmongoclient):
    """Test ValidateMetadata class. Run task for dataset that has valid
    metadata."""

    # Create "logs" directory
    os.mkdir(os.path.join(testpath, 'logs'))

    # Init task
    task = ValidateMetadata(workspace=testpath,
                            dataset_id='validate_metadata_test_dataset_1',
                            config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task
    task.run()
    assert task.complete()


def test_invalid_metadata(testpath, testmetax, testmongoclient):
    """Test ValidateMetadata class. Run task for dataset that has invalid
    metadata. The dataset is missing attribute: 'type' for each object in files
    list."""

    # Create "logs" directory
    os.mkdir(os.path.join(testpath, 'logs'))

    # Init task
    task = ValidateMetadata(workspace=testpath,
                            dataset_id='validate_metadata_test_dataset_2',
                            config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "'contract' is a required property" in exc.value[0]
    assert not task.complete()


def test_missing_xml_metadata(testpath, testmetax, testmongoclient):
    """Test ValidateMetadata class. Run task for dataset missing a
    xml metadata file for an image file."""

    # Create "logs" directory
    os.mkdir(os.path.join(testpath, 'logs'))

    # Init task
    task = ValidateMetadata(workspace=testpath,
                            dataset_id='validate_metadata_test_dataset_3',
                            config='tests/data/siptools_research.conf')
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidMetadataError):
        task.run()

    assert not task.complete()
