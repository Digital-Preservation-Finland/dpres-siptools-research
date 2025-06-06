"""Tests for :mod:`siptools_research.workflowtask` module."""
import copy

import luigi
import pytest
import requests
from metax_access import (
    DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    DS_STATE_GENERATING_METADATA,
    DS_STATE_INVALID_METADATA,
    DS_STATE_PACKAGING_FAILED,
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
)

from siptools_research.dataset import Dataset, find_datasets
from siptools_research.exceptions import (
    InvalidContractMetadataError,
    InvalidDatasetError,
    InvalidDatasetFileError,
    InvalidDatasetMetadataError,
    InvalidFileError,
    InvalidFileMetadataError,
    InvalidSIPError,
    MissingFileError,
)
from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.models.file_error import FileError
from tests.metax_data.datasets import BASE_DATASET


# pylint: disable=too-few-public-methods
class FalseTarget(luigi.Target):
    """Dummy target that never exists."""

    def exists(self):
        """Return False."""
        return False


class DummyTask(WorkflowTask):
    """Test class that only writes an output file."""

    success_message = 'Test task was successful'

    def output(self):
        """Create output file.

        :returns: local target: `<workspace>/preservation/output_file`
        """
        return luigi.LocalTarget(
            str(self.dataset.preservation_workspace / 'output_file')
        )

    def run(self):
        """Write something to output file.

        :returns:  ``None``
        """
        with self.output().open('w') as outputfile:
            outputfile.write('Hello world')


class FailingTask(WorkflowTask):
    """Test class that always fails."""

    failure_message = 'An error occurred while running a test task'

    def output(self):
        """Output never exists."""
        return FalseTarget()

    def run(self):
        """Raise exception.

        :returns:  ``None``
        """
        error = "Shit hit the fan"
        raise ValueError(error)


class InvalidDatasetTask(FailingTask):
    """Test class that raises InvalidDatasetError."""

    def run(self):
        """Raise InvalidDatasetError.

        :returns:  ``None``
        """
        error = "Dataset is invalid"
        raise InvalidDatasetError(error)


class InvalidDatasetMetadataTask(FailingTask):
    """Test class that raises InvalidDatasetMetadataError."""

    def run(self):
        """Raise InvalidDatasetMetadataError.

        :returns:  ``None``
        """
        error = "Missing some important metadata"
        raise InvalidDatasetMetadataError(error)


class InvalidFileMetadataTask(FailingTask):
    """Test class that raises InvalidFileMetadataError."""

    def run(self):
        """Raise InvalidFileMetadataError.

        :returns:  ``None``
        """
        error = "Invalid file encoding"
        raise InvalidFileMetadataError(error, files=[{"id": "foo"}])


class InvalidContractMetadataTask(FailingTask):
    """Test class that raises InvalidContractMetadataError."""

    def run(self):
        """Raise InvalidContractMetadataError.

        :returns: ``None``
        """
        error = "Missing organization identifier"
        raise InvalidContractMetadataError(error)


class MissingFileTask(FailingTask):
    """Test class that raises MissingFileError."""

    def run(self):
        """Raise MissingFileError.

        :returns:  ``None``
        """
        error = "A file was not found in Ida"
        raise MissingFileError(error, files=[{"id": "foo"}])


class InvalidFileTask(FailingTask):
    """Test class that raises InvalidFileError."""

    def run(self):
        """Raise InvalidFileError.

        :returns:  ``None``
        """
        error = "A file is not well-formed"
        raise InvalidFileError(error, files=[{"id": "foo"}])


class InvalidSIPTask(FailingTask):
    """Test class that raises InvalidSIPError."""

    def run(self):
        """Raise InvalidSIPError.

        :returns:  ``None``
        """
        error = "SIP was rejected in DPS"
        raise InvalidSIPError(error)


class MetaxTask(WorkflowTask):
    """Test class that retrieves dataset from Metax."""

    failure_message = 'Failed retrieving dataste from Metax'

    def output(self):
        """Output never exists."""
        return FalseTarget()

    def run(self):
        """Get dataset 1 from Metax.

        :returns:  ``None``
        """
        metax_client = get_metax_client(self.config)
        metax_client.get_dataset('1')


@pytest.mark.usefixtures('mock_luigi_config_path')
def test_run_workflowtask(config, workspace):
    """Test WorkflowTask execution.

    Executes a DummyTask, checks that output file is created, checks
    that new task is added to task log.

    :param workspace: temporary directory
    :returns: ``None``
    """
    # Add a workflow to database
    dataset = Dataset(workspace.name, config=config)
    dataset.preserve()

    # Run DummyTask
    luigi.build(
        [DummyTask(workspace.name, config=config)],
        local_scheduler=True
    )

    # Check that output file is created
    assert (workspace / "preservation" / "output_file").read_text() \
        == "Hello world"

    dataset = Dataset(workspace.name, config=config)
    tasks = dataset.get_tasks()
    # Check 'messages' field
    assert tasks['DummyTask']['messages'] == 'Test task was successful'
    # Check 'result' field
    assert tasks['DummyTask']['result'] == 'success'

    # Workflow should not be disabled
    assert dataset.enabled

    # Check that there is no extra workflows in database
    assert len(find_datasets(config=config)) == 1


@pytest.mark.usefixtures('mock_luigi_config_path')
def test_run_workflow_target_task(config, workspace):
    """Test running target task of the workflow.

    Create a workflow with DummyTask as target Task. Check that workflow
    is disabled after executing the task.

    :param config: Configuration file
    :param workspace: temporary directory
    """
    # Add workflow to database
    Dataset(workspace.name, config=config).preserve()

    # Run DummyTask
    luigi.build(
        [DummyTask(workspace.name,
                   config=config,
                   is_target_task=True)],
        local_scheduler=True
    )

    # Check that new task is added to task log
    dataset = Dataset(workspace.name, config=config)
    tasks = dataset.get_tasks()
    assert tasks['DummyTask']['result'] == 'success'

    # Workflow should be disabled
    assert not dataset.enabled


def test_run_failing_task(config, workspace):
    """Test running task that fails.

    Executes FailingTask and checks that report of failed task is added
    to task log.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    """
    # Run FailingTask
    luigi.build(
        [FailingTask(workspace.name, config=config)],
        local_scheduler=True
    )

    # Check that new task is added to task log
    tasks = Dataset(workspace.name, config=config).get_tasks()
    # Check 'messages' field
    assert tasks['FailingTask']['messages'] \
        == 'An error occurred while running a test task: Shit hit the fan'
    # Check 'result' field
    assert tasks['FailingTask']['result'] \
        == 'failure'


@pytest.mark.parametrize(
    ('task', 'expected_state', 'expected_description'),
    [

        (
            InvalidDatasetTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidDatasetError: Dataset is invalid'
        ),
        (
            InvalidDatasetMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidDatasetMetadataError: Missing some important metadata'
        ),
        (
            InvalidFileMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidFileMetadataError: Invalid file encoding'
        ),
        (
            InvalidContractMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidContractMetadataError: Missing organization identifier'
        ),
        (
            InvalidFileTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidFileError: A file is not well-formed'
        ),
        (
            MissingFileTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'MissingFileError: A file was not found in Ida'
        ),
        (
            InvalidSIPTask,
            DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
            'An error occurred while running a test task: '
            'InvalidSIPError: SIP was rejected in DPS'
        ),
    ]
)
def test_invalid_dataset_error(config, workspace, requests_mock, task,
                               expected_state, expected_description):
    """Test event handler of WorkflowTask.

    Event handler should report preservation state to Metax if
    InvalidDatasetError raises in a task.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: Mocker object
    :param task: Test task to be run
    :param expected_state: Preservation state that should be reported to
                           Metax
    :param expected_description: Preservation description that should
                                 be reported to Metax
    """
    # Mock metax
    json=copy.deepcopy(BASE_DATASET)
    json["id"] = workspace.name
    json["preservation"]["state"] = DS_STATE_GENERATING_METADATA
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=json)
    patch_preservation = requests_mock.patch(
        f"/v3/datasets/{workspace.name}/preservation"
    )

    # Run the task
    luigi.build(
        [task(workspace.name, config=config)],
        local_scheduler=True
    )

    # Check that expected preservation state was set to Metax
    assert patch_preservation.called_once
    assert patch_preservation.last_request.json() == {
        "state": expected_state,
        "description": {"en": expected_description}
    }


def test_file_error_saved_fields(config, workspace, requests_mock):
    """
    Test that workflow file error database will be updated when a task
    fails due to a file-related error and that all fields exist
    """
    class TestTask(FailingTask):
        def run(self):
            raise InvalidDatasetFileError(
                "Failed to reticulate splines",
                files=[
                    {
                        "id": "file-id",
                        "storage_identifier": "storage-id",
                        "storage_service": "pas"
                    }
                ]
            )

    # Mock metax
    json=copy.deepcopy(BASE_DATASET)
    json["id"] = workspace.name
    json["preservation"]["state"] = DS_STATE_GENERATING_METADATA
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=json)
    requests_mock.patch(
        f"/v3/datasets/{workspace.name}/preservation"
    )

    luigi.build(
        [TestTask(dataset_id=workspace.name, config=config)],
        local_scheduler=True
    )

    assert FileError.objects.count() == 1

    file_error = FileError.objects.all()[0]

    assert file_error.file_id == "file-id"
    assert file_error.storage_identifier == "storage-id"
    assert file_error.storage_service.value == "pas"
    assert file_error.dataset_id is None
    assert file_error.created_at.isoformat()  # 'created_at' is a date


@pytest.mark.parametrize(
    "exception,expected_file_errors",
    [
        (
            InvalidDatasetFileError(
                "Failed to reticulate splines",
                files=[
                    {
                        "id": "file-id",
                        "storage_identifier": "storage-id",
                        "storage_service": "pas"
                    }
                ]
            ),
            [
                {
                    "file_id": "file-id",
                    "storage_identifier": "storage-id",
                    "storage_service": "pas",
                    "dataset_id": None,
                    "errors": ["Failed to reticulate splines"]
                }
            ]
        ),
        (
            InvalidDatasetFileError(
                "Failed to reticulate splines",
                files=[
                    {
                        "id": "file-id-1",
                        "storage_identifier": "storage-id-1",
                        "storage_service": "pas"
                    },
                    {
                        "id": "file-id-2",
                        "storage_identifier": "storage-id-2",
                        "storage_service": "ida"
                    }
                ],
                is_dataset_error=True
            ),
            [
                {
                    "file_id": "file-id-1",
                    "storage_identifier": "storage-id-1",
                    "storage_service": "pas",
                    "dataset_id": "dataset-id",
                    "errors": ["Failed to reticulate splines"]
                },
                {
                    "file_id": "file-id-2",
                    "storage_identifier": "storage-id-2",
                    "storage_service": "ida",
                    "dataset_id": "dataset-id",
                    "errors": ["Failed to reticulate splines"]
                }
            ]
        )
    ]
)
def test_file_error_saved(
        config, workspace, requests_mock, exception, expected_file_errors):
    """
    Test that workflow file error database will be updated when a task
    fails due to a file-related error
    """
    class TestTask(FailingTask):
        def run(self):
            raise exception

    # Mock metax
    json=copy.deepcopy(BASE_DATASET)
    json["id"] = "dataset-id"
    json["preservation"]["state"] = DS_STATE_GENERATING_METADATA
    requests_mock.get("/v3/datasets/dataset-id", json=json)
    requests_mock.patch(
        "/v3/datasets/dataset-id/preservation"
    )

    luigi.build(
        [TestTask(dataset_id="dataset-id", config=config)],
        local_scheduler=True
    )

    # Ensure the correct file error was written into the database
    for error in expected_file_errors:
        assert FileError.objects.filter(**error).count() == 1


def test_set_preservation_state_of_pas_version(config, requests_mock):
    """Test that preservation state of correct dataset version is set.

    If the dataset has been copied to PAS data catalog, the preservation
    state of the PAS version should be set.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock metax
    json = copy.deepcopy(BASE_DATASET)
    json["id"] = "original-id"
    json["preservation"]["dataset_version"] = {
        "id": "pas-version-id",
        "persistent_identifier": None,
        "preservation_state": 0,
    }
    requests_mock.get("/v3/datasets/original-id", json=json)
    patch_preservation \
        = requests_mock.patch("/v3/datasets/pas-version-id/preservation")

    # Run the task
    luigi.build(
        [InvalidDatasetTask(dataset_id="original-id", config=config)],
        local_scheduler=True
    )

    # The preservation state of PAS version of the dataset should be set
    assert patch_preservation.called_once
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_INVALID_METADATA,
        "description":{
            "en": "An error occurred while running a test task: "
            "InvalidDatasetError: Dataset is invalid"
        }
    }


def test_packaging_failed(config, workspace, requests_mock):
    """Test failure during packaging.

    If packaging fails because dataset is invalid, preservation state
    should be set to DS_STATE_PACKAGING_FAILED instead of
    DS_STATE_INVALID_METADATA. See TPASPKT-998 for more information.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock: Mocker object
    """
    # Mock Metax
    json=copy.deepcopy(BASE_DATASET)
    json["id"] = workspace.name
    # DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION means that packaging has
    # started
    json["preservation"]["state"] = DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
    requests_mock.get(f"/v3/datasets/{workspace.name}", json=json)
    patch_preservation = requests_mock.patch(
        f"/v3/datasets/{workspace.name}/preservation"
    )

    # Run InvalidDatasetTask
    luigi.build(
        [InvalidDatasetTask(workspace.name, config=config)],
        local_scheduler=True
    )

    assert patch_preservation.called_once
    assert patch_preservation.last_request.json() == {
        "state": DS_STATE_PACKAGING_FAILED,
        "description": {
            "en": "An error occurred while running a test task: "
            "InvalidDatasetError: Dataset is invalid"
        }
    }


def test_logging(config, workspace, requests_mock, caplog):
    """Test logging failed HTTP responses.

    :param config: Configuration file
    :param workspace: Temporary workspace directory
    :param requests_mock HTTP request mocker
    :param caplog: Captured log messages
    """
    # Mock Metax
    requests_mock.get("/v3/datasets/1",
                      status_code=403,
                      reason="Access denied",
                      text="No rights to view dataset")

    # Run task that sends HTTP request
    luigi.build([MetaxTask(workspace.name, config=config)],
                local_scheduler=True)

    # Check errors in logs
    errors = [r for r in caplog.records if r.levelname == 'ERROR']

    # First error should contain the the body of response to failed
    # request
    error_message = errors[0].getMessage()

    assert error_message.startswith(
        "HTTP request to https://metax.localhost"
    )
    assert error_message.endswith(
        "Response from server was: No rights to view dataset"
    )

    # Second logged error should be the raised HTTPError
    assert errors[1].exc_text.startswith('Traceback ')
    exception = errors[1].exc_info[1]
    assert isinstance(exception, requests.HTTPError)
    assert str(exception).startswith('403 Client Error: Access denied')


# TODO: Test for WorkfloExternalTask
