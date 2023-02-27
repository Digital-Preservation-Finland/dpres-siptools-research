"""Unit tests for ReportDatasetValidationResult task."""

import pytest
from metax_access.metax import DS_STATE_VALID_METADATA

import tests
from siptools_research.workflow import report_dataset_validation_result


@pytest.mark.usefixtures('testmongoclient')
def test_reportdatasetvalidationresult(testpath, requests_mock):
    """Test reporting dataset validation result.

    :param testpath: Temporary directory
    :param requests_mock: HTTP request mocker
    :returns: ``None``
    """
    # Mock Metax
    requests_mock.get('/rest/v2/datasets/foobar', json={})
    patch_dataset = requests_mock.patch('/rest/v2/datasets/foobar')

    # Init and run task
    task = report_dataset_validation_result.ReportDatasetValidationResult(
        workspace=str(testpath),
        dataset_id="foobar",
        config=tests.conftest.UNIT_TEST_CONFIG_FILE
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # The dataset preservation state should be updated
    assert patch_dataset.called_once
    assert patch_dataset.last_request.json() \
        == {'preservation_state': DS_STATE_VALID_METADATA,
            'preservation_description':
            'Metadata and files validated'}
