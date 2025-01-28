"""Unit tests for ReportDatasetValidationResult task."""
import pytest
from metax_access.metax import DS_STATE_METADATA_CONFIRMED

import tests.metax_data
from siptools_research.workflow import report_dataset_validation_result


@pytest.mark.usefixtures('testmongoclient')
def test_reportdatasetvalidationresult(config, workspace, requests_mock,
                                       request):
    """Test reporting dataset validation result.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    :param request: Pytest CLI arguments
    """
    # Mock Metax API V3
    dataset = tests.metax_data.datasetsV3.BASE_DATASET
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    patch_dataset \
        = requests_mock.patch(f"/v3/datasets/{workspace.name}/preservation")

    # Mock Metax API V2
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
    v2_patch_dataset = requests_mock.patch('/rest/v2/datasets/foo')

    # Init and run task
    task = report_dataset_validation_result.ReportDatasetValidationResult(
        dataset_id=workspace.name,
        config=config
    )
    assert not task.complete()
    task.run()
    assert task.complete()

    # The dataset preservation state should be updated
    if request.config.getoption("--v3"):
        assert patch_dataset.called_once
        assert patch_dataset.last_request.json() \
            == {"state": DS_STATE_METADATA_CONFIRMED,
                "description": {"en": "Metadata and files validated"}}
    else:
        assert v2_patch_dataset.called_once
        assert v2_patch_dataset.last_request.json() \
            == {'preservation_state': DS_STATE_METADATA_CONFIRMED,
                'preservation_description':
                'Metadata and files validated'}
