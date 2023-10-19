"""Unit tests for ReportDatasetValidationResult task."""

import pytest
from metax_access.metax import DS_STATE_METADATA_CONFIRMED

import tests
from siptools_research.workflow import report_dataset_validation_result


@pytest.mark.usefixtures('testmongoclient')
def test_reportdatasetvalidationresult(workspace, requests_mock):
    """Test reporting dataset validation result.

    :param testpath: Temporary directory
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get(f'/rest/v2/datasets/{workspace.name}', json={})
    patch_dataset = requests_mock.patch(f'/rest/v2/datasets/{workspace.name}')

    # Init and run task
    task = report_dataset_validation_result.ReportDatasetValidationResult(
        dataset_id=workspace.name,
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # The dataset preservation state should be updated
    assert patch_dataset.called_once
    assert patch_dataset.last_request.json() \
        == {'preservation_state': DS_STATE_METADATA_CONFIRMED,
            'preservation_description':
            'Metadata and files validated'}
