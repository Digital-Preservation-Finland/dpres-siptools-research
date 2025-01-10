"""Unit tests for CopyToPasDataCatalog task."""
import pytest
from metax_access import (
    DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
)

from siptools_research.workflow import copy_dataset_to_pas_data_catalog


@pytest.mark.usefixtures('testmongoclient')
def test_copy_ida_dataset(config, workspace, requests_mock):
    """Test copying Ida dataset to PAS data catalog.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-ida"
            },
            'identifier': 'original-version-id',
            'preservation_state': DS_STATE_METADATA_CONFIRMED,
            "research_dataset": {
                    "files": [
                        {
                            "details": {
                                "project_identifier": "foo"
                            }
                        }
                    ]
                }
        }
    )
    metax_mock = requests_mock.patch('/rest/v2/datasets/original-version-id')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.called_once
    assert metax_mock.last_request.json() == {
        'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        'preservation_description':
        'Packaging dataset'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_ida_dataset_already_copied(config, workspace, requests_mock):
    """Test running task when dataset has already been copied.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-ida"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_dataset_version': {
                'preferred_identifier': 'pas-version-id',
                'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            },
            'preservation_state': DS_STATE_INITIALIZED
        }
    )
    metax_mock = requests_mock.patch(f'/rest/v2/datasets/{workspace.name}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called


@pytest.mark.usefixtures('testmongoclient')
def test_copy_pas_dataset(config, workspace, requests_mock):
    """Test running task for pottumounttu dataset.

    The dataset was originally created in PAS catalog. So it is not
    actually copied anywhere, but preservation state is updated.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'identifier': 'original-version-id',
            'preservation_state': DS_STATE_METADATA_CONFIRMED,
            "research_dataset": {
                    "files": [
                        {
                            "details": {
                                "project_identifier": "foo"
                            }
                        }
                    ]
                }
        }
    )
    metax_mock = requests_mock.patch('/rest/v2/datasets/original-version-id')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.last_request.json() == {
        'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        'preservation_description':
        'Packaging dataset'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_pas_dataset_already_copied(config, workspace, requests_mock):
    """Test copying PAS dataset to PAS data catalog.

    The dataset was originally created in PAS catalog, and the
    preservation state has already been updated, so the
    CopyToPasDataCatalog task does not really do anything.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
        }
    )
    metax_mock = requests_mock.patch(f'/rest/v2/datasets/{workspace.name}')

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called
