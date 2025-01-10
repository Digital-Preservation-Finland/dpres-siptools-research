"""Unit tests for ReportDatasetValidationResult task."""

import pytest
from metax_access.metax import DS_STATE_METADATA_CONFIRMED

from siptools_research.workflow import report_dataset_validation_result


@pytest.mark.usefixtures('testmongoclient')
def test_reportdatasetvalidationresult(config, workspace, requests_mock):
    """Test reporting dataset validation result.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={'identifier': 'foo',
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
    patch_dataset = requests_mock.patch('/rest/v2/datasets/foo')

    # Init and run task
    task = report_dataset_validation_result.ReportDatasetValidationResult(
        dataset_id=workspace.name,
        config=config
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
