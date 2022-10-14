"""Tests for :mod:`siptools_research.workflow.validate_metadata`."""

import copy

import pytest

from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata
import tests.conftest
from tests.utils import add_metax_dataset
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TXT_FILE


@pytest.mark.usefixtures('testmongoclient', 'mock_filetype_conf')
def test_validatemetadata(workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has valid metadata.

    :param workspace: Temporary directory fixture
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax. Create a valid dataset that contains two text files.
    file1 = copy.deepcopy(TXT_FILE)
    file1['identifier'] = 'identifier1'
    file1['file_path'] = '/path1'
    file2 = copy.deepcopy(TXT_FILE)
    file2['identifier'] = 'identifier2'
    file2['file_path'] = '/'
    add_metax_dataset(requests_mock=requests_mock, files=[file1, file2])

    # Init task
    task = ValidateMetadata(workspace=str(workspace),
                            dataset_id='dataset_identifier',
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()

    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_invalid_metadata(workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has invalid metadata. The dataset is
    missing attribute: 'type' for each object in files list.

    :param workspace: Temporary workspace directory fixture
    :returns: ``None``
    """
    # Mock Metax. Remove "contract" from dataset metadata to create an
    # invalid dataset.
    dataset = copy.deepcopy(BASE_DATASET)
    del dataset['contract']
    requests_mock.get('/rest/v2/datasets/dataset_identifier', json=dataset)

    # Init task
    task = ValidateMetadata(
        workspace=str(workspace),
        dataset_id='dataset_identifier',
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidDatasetMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "'contract' is a required property" in str(exc.value)
    assert not task.complete()
