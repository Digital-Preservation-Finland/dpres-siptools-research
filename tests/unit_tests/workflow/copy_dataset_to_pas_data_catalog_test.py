"""Unit tests for CopyToPasDataCatalog task."""
import pytest
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_METADATA_CONFIRMED,
                          DS_STATE_INITIALIZED)

import tests.conftest
from siptools_research.workflow import copy_dataset_to_pas_data_catalog


@pytest.mark.usefixtures('testmongoclient')
def test_copy_ida_dataset(testpath, requests_mock):
    """Test copying Ida dataset to PAS data catalog.

    :param testpath: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(
        '/rest/v2/datasets/foobar',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-ida"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_state': DS_STATE_METADATA_CONFIRMED
        }
    )
    metax_mock = requests_mock.patch('/rest/v2/datasets/foobar')

    workspace = testpath

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        workspace=str(workspace),
        dataset_id="foobar",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.called_once
    assert metax_mock.last_request.json() == {
        'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        'preservation_description':
        'Accepted to preservation by packaging service'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_ida_dataset_already_copied(testpath, requests_mock):
    """Test running task when dataset has already been copied.

    :param testpath: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(
        '/rest/v2/datasets/foobar',
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
    metax_mock = requests_mock.patch('/rest/v2/datasets/foobar')

    workspace = testpath

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        workspace=str(workspace),
        dataset_id="foobar",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called


@pytest.mark.usefixtures('testmongoclient')
def test_copy_pas_dataset(testpath, requests_mock):
    """Test running task for pottumounttu dataset.

    The dataset was originally created in PAS catalog. So it is not
    actually copied anywhere, but preservation state is updated.

    :param testpath: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(
        '/rest/v2/datasets/foobar',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_state': DS_STATE_METADATA_CONFIRMED
        }
    )
    metax_mock = requests_mock.patch('/rest/v2/datasets/foobar')

    workspace = testpath

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        workspace=str(workspace),
        dataset_id="foobar",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert metax_mock.last_request.json() == {
        'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
        'preservation_description':
        'Accepted to preservation by packaging service'
    }


@pytest.mark.usefixtures('testmongoclient')
def test_pas_dataset_already_copied(testpath, requests_mock):
    """Test copying Ida dataset to PAS data catalog.

    The dataset was originally created in PAS catalog, and the
    preservation state has already been updated, so the
    CopyToPasDataCatalog task does not really do anything.

    :param testpath: Temporary directory fixture
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(
        '/rest/v2/datasets/foobar',
        json={
            'data_catalog': {
                'identifier': "urn:nbn:fi:att:data-catalog-pas"
            },
            'preservation_identifier': 'original-version-id',
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
        }
    )
    metax_mock = requests_mock.patch('/rest/v2/datasets/foobar')

    workspace = testpath

    # Init and run task
    task = copy_dataset_to_pas_data_catalog.CopyToPasDataCatalog(
        workspace=str(workspace),
        dataset_id="foobar",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    assert not metax_mock.called
