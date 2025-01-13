"""Tests for :mod:`siptools_research.workflow.validate_metadata`."""

import copy

import pytest

import tests.conftest
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.workflow.validate_metadata import ValidateMetadata
from tests.metax_data.datasets import BASE_DATASET
from tests.metax_data.files import TXT_FILE
from tests.utils import add_metax_v2_dataset


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
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    add_metax_v2_dataset(
        requests_mock=requests_mock,
        dataset=dataset,
        files=[file1, file2]
    )

    # Init task
    task = ValidateMetadata(dataset_id=workspace.name,
                            config=tests.conftest.UNIT_TEST_CONFIG_FILE)
    assert not task.complete()

    # Run task
    task.run()

    assert task.complete()


@pytest.mark.usefixtures('testmongoclient')
def test_invalid_metadata(workspace, requests_mock):
    """Test ValidateMetadata class.

    Run task for dataset that has invalid metadata that does
    not pass schema validation.

    :param workspace: Temporary workspace directory fixture
    :returns: ``None``
    """
    # Mock Metax. Remove "contract" from dataset metadata to create an
    # invalid dataset.
    dataset = copy.deepcopy(BASE_DATASET)
    dataset['identifier'] = workspace.name
    del dataset['contract']
    requests_mock.get(f'/rest/v2/datasets/{workspace.name}', json=dataset)

    # Init task
    task = ValidateMetadata(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()

    # Run task
    with pytest.raises(InvalidDatasetMetadataError) as exc:
        task.run()

    # run should fail the following error message:
    assert "None is not of type 'string'" in str(exc.value)
    assert not task.complete()
