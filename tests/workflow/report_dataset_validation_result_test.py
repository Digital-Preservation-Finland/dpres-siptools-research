"""Unit tests for ReportDatasetValidationResult task."""
from metax_access.metax import DS_STATE_DATASET_VALIDATED

from siptools_research.workflow import report_dataset_validation_result
from metax_access.template_data import DATASET


def test_reportdatasetvalidationresult(config, workspace, requests_mock):
    """Test reporting dataset validation result.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    dataset = DATASET
    dataset["id"] = workspace.name
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=dataset)
    patch_dataset \
        = requests_mock.patch(f"/v3/datasets/{workspace.name}/preservation")

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
        == {"state": DS_STATE_DATASET_VALIDATED,
            "description": {"en": "Metadata and files validated"}}
