"""Tests for :mod:`siptools_research.workflowtask` module."""

import luigi
import pytest
import requests
from metax_access import (DS_STATE_GENERATING_METADATA,
                          DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_INVALID_METADATA,
                          DS_STATE_PACKAGING_FAILED,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)
from siptools_research.metax import get_metax_client

from tests.conftest import UNIT_TEST_CONFIG_FILE
from siptools_research.dataset import Dataset, find_datasets
from siptools_research.exceptions import (InvalidDatasetError,
                                          InvalidDatasetMetadataError,
                                          InvalidFileMetadataError,
                                          InvalidContractMetadataError,
                                          InvalidFileError,
                                          MissingFileError,
                                          InvalidSIPError)
from siptools_research.workflowtask import WorkflowTask


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
        raise Exception('Shit hit the fan')


class InvalidDatasetTask(FailingTask):
    """Test class that raises InvalidDatasetError."""

    def run(self):
        """Raise InvalidDatasetError.

        :returns:  ``None``
        """
        raise InvalidDatasetError('Dataset is invalid')


class InvalidDatasetMetadataTask(FailingTask):
    """Test class that raises InvalidDatasetMetadataError."""

    def run(self):
        """Raise InvalidDatasetMetadataError.

        :returns:  ``None``
        """
        raise InvalidDatasetMetadataError('Missing some important metadata')


class InvalidFileMetadataTask(FailingTask):
    """Test class that raises InvalidFileMetadataError."""

    def run(self):
        """Raise InvalidFileMetadataError.

        :returns:  ``None``
        """
        raise InvalidFileMetadataError('Invalid file encoding')


class InvalidContractMetadataTask(FailingTask):
    """Test class that raises InvalidContractMetadataError."""

    def run(self):
        """Raise InvalidContractMetadataError.

        :returns: ``None``
        """
        raise InvalidContractMetadataError('Missing organization identifier')


class MissingFileTask(FailingTask):
    """Test class that raises MissingFileError."""

    def run(self):
        """Raise MissingFileError.

        :returns:  ``None``
        """
        raise MissingFileError('A file was not found in Ida')


class InvalidFileTask(FailingTask):
    """Test class that raises InvalidFileError."""

    def run(self):
        """Raise InvalidFileError.

        :returns:  ``None``
        """
        raise InvalidFileError('A file is not well-formed')


class InvalidSIPTask(FailingTask):
    """Test class that raises InvalidSIPError."""

    def run(self):
        """Raise InvalidSIPError.

        :returns:  ``None``
        """
        raise InvalidSIPError('SIP was rejected in DPS')


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


@pytest.mark.usefixtures('mock_luigi_config_path', 'testmongoclient')
def test_run_workflowtask(workspace):
    """Test WorkflowTask execution.

    Executes a DummyTask, checks that output file is created, checks
    that new task is added to task log.

    :param workspace: temporary directory
    :returns: ``None``
    """
    # Add a workflow to database
    dataset = Dataset(workspace.name, config=UNIT_TEST_CONFIG_FILE)
    dataset.preserve()

    # Run DummyTask
    luigi.build(
        [DummyTask(workspace.name, config=UNIT_TEST_CONFIG_FILE)],
        local_scheduler=True
    )

    # Check that output file is created
    assert (workspace / "preservation" / "output_file").read_text() \
        == "Hello world"

    dataset = Dataset(workspace.name, config=UNIT_TEST_CONFIG_FILE)
    tasks = dataset.get_tasks()
    # Check 'messages' field
    assert tasks['DummyTask']['messages'] == 'Test task was successful'
    # Check 'result' field
    assert tasks['DummyTask']['result'] == 'success'

    # Workflow should not be disabled
    assert dataset.enabled

    # Check that there is no extra workflows in database
    assert len(find_datasets(config=UNIT_TEST_CONFIG_FILE)) == 1


@pytest.mark.usefixtures('mock_luigi_config_path', 'testmongoclient')
def test_run_workflow_target_task(workspace):
    """Test running target task of the workflow.

    Create a workflow with DummyTask as target Task. Check that workflow
    is disabled after executing the task.

    :param workspace: temporary directory
    :returns: ``None``
    """
    # Add workflow to database
    Dataset(workspace.name, config=UNIT_TEST_CONFIG_FILE).preserve()

    # Run DummyTask
    luigi.build(
        [DummyTask(workspace.name,
                   config=UNIT_TEST_CONFIG_FILE,
                   is_target_task=True)],
        local_scheduler=True
    )

    # Check that new task is added to task log
    dataset = Dataset(workspace.name, config=UNIT_TEST_CONFIG_FILE)
    tasks = dataset.get_tasks()
    assert tasks['DummyTask']['result'] == 'success'

    # Workflow should be disabled
    assert not dataset.enabled


@pytest.mark.usefixtures('testmongoclient')
def test_run_failing_task(workspace):
    """Test running task that fails.

    Executes FailingTask and checks that report of failed task is added
    to task log.

    :param workspace: Temporary workspace directory
    :returns: ``None``
    """
    # Run FailingTask
    luigi.build(
        [FailingTask(workspace.name, config=UNIT_TEST_CONFIG_FILE)],
        local_scheduler=True
    )

    # Check that new task is added to task log
    tasks = Dataset(workspace.name, config=UNIT_TEST_CONFIG_FILE).get_tasks()
    # Check 'messages' field
    assert tasks['FailingTask']['messages'] \
        == 'An error occurred while running a test task: Shit hit the fan'
    # Check 'result' field
    assert tasks['FailingTask']['result'] \
        == 'failure'


@pytest.mark.parametrize(
    ('task', 'expected_state', 'expected_description'),
    (

        [
            InvalidDatasetTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidDatasetError: Dataset is invalid'
        ],
        [
            InvalidDatasetMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidDatasetMetadataError: Missing some important metadata'
        ],
        [
            InvalidFileMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidFileMetadataError: Invalid file encoding'
        ],
        [
            InvalidContractMetadataTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidContractMetadataError: Missing organization identifier'
        ],
        [
            InvalidFileTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidFileError: A file is not well-formed'
        ],
        [
            MissingFileTask,
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'MissingFileError: A file was not found in Ida'
        ],
        [
            InvalidSIPTask,
            DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
            'An error occurred while running a test task: '
            'InvalidSIPError: SIP was rejected in DPS'
        ],
    )
)
@pytest.mark.usefixtures('testmongoclient')
def test_invalid_dataset_error(workspace, requests_mock, task, expected_state,
                               expected_description):
    """Test event handler of WorkflowTask.

    Event handler should report preservation state to Metax if
    InvalidDatasetError raises in a task.

    :param workspace: Temporary workspace directory
    :param requests_mock: Mocker object
    :param task: Test task to be run
    :param expected_state: Preservation state that should be reported to
                           Metax
    :param expected_description: Preservation description that should
                                 be reported to Metax
    :returns: ``None``
    """
    # Mock metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'identifier': 'test123',
            'preservation_state': DS_STATE_GENERATING_METADATA,
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
    patch_dataset_api = requests_mock.patch('/rest/v2/datasets/test123')

    # Run the task
    luigi.build(
        [task(workspace.name, config=UNIT_TEST_CONFIG_FILE)],
        local_scheduler=True
    )

    # Check that expected preservation state was set
    assert patch_dataset_api.called_once
    assert patch_dataset_api.last_request.json() == {
        'preservation_state': expected_state,
        'preservation_description': expected_description
    }
    assert patch_dataset_api.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient')
def test_set_preservation_state_of_pas_version(requests_mock):
    """Test that preservation state of correct dataset version is set.

    If the dataset has been copied to PAS data catalog, the preservation
    state of the PAS version should be set.

    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax
    requests_mock.get(
        '/rest/v2/datasets/original-id',
        json={
            "identifier": "original-id",
            "preservation_dataset_version": {
                "identifier": "pas-version-id",
                "preservation_state": DS_STATE_GENERATING_METADATA
            },
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
    patch_dataset_api = requests_mock.patch('/rest/v2/datasets/pas-version-id')

    # Run the task
    luigi.build(
        [InvalidDatasetTask(dataset_id='original-id',
                            config=UNIT_TEST_CONFIG_FILE)],
        local_scheduler=True
    )

    # The preservation state of PAS version of the dataset should be
    # set
    assert patch_dataset_api.called_once
    assert patch_dataset_api.last_request.json() == {
        'preservation_state': DS_STATE_INVALID_METADATA,
        'preservation_description': ('An error occurred while running a '
                                     'test task: InvalidDatasetError: '
                                     'Dataset is invalid')
    }
    assert patch_dataset_api.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient')
def test_packaging_failed(workspace, requests_mock):
    """Test failure during packaging.

    If packaging fails because dataset is invalid, preservation state
    should be set to DS_STATE_PACKAGING_FAILED.

    :param workspace: Temporary workspace directory
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Mock metax
    requests_mock.get(
        f'/rest/v2/datasets/{workspace.name}',
        json={
            'identifier': 'test123',
            'preservation_state': DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
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
    patch_dataset_api = requests_mock.patch('/rest/v2/datasets/test123')

    # Run InvalidDatasetTask
    luigi.build(
        [InvalidDatasetTask(workspace.name, config=UNIT_TEST_CONFIG_FILE)],
        local_scheduler=True
    )

    # Check that the preservation was set correctly
    assert patch_dataset_api.called_once
    assert patch_dataset_api.last_request.json() == {
        'preservation_state': DS_STATE_PACKAGING_FAILED,
        'preservation_description': ('An error occurred while running a '
                                     'test task: InvalidDatasetError: '
                                     'Dataset is invalid')
    }
    assert patch_dataset_api.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient')
def test_logging(workspace, requests_mock, caplog):
    """Test logging failed HTTP responses.

    :param workspace: Temporary workspace directory
    :param responses: HTTP request mocker
    :param caplog: Captured log messages
    """
    # Create mocked response for HTTP request.
    requests_mock.get('https://metaksi/rest/v2/datasets/1',
                      status_code=403,
                      reason='Access denied',
                      text='No rights to view dataset')

    # Run task that sends HTTP request
    luigi.build([MetaxTask(workspace.name, config=UNIT_TEST_CONFIG_FILE)],
                local_scheduler=True)

    # Check errors in logs
    errors = [r for r in caplog.records if r.levelname == 'ERROR']

    # First error should contain the the body of response to failed
    # request
    error_message = errors[0].getMessage()

    assert error_message.startswith(
        "HTTP request to https://metaksi/rest/v2/datasets/1?"
    )
    assert error_message.endswith(
        "Response from server was: No rights to view dataset"
    )

    # Second logged error should be the raised HTTPError
    assert errors[1].exc_text.startswith('Traceback ')
    exception = errors[1].exc_info[1]
    assert isinstance(exception, requests.HTTPError)
    assert str(exception).startswith('403 Client Error: Access denied')


# TODO: Test for WorkflowWrapperTask

# TODO: Test for WorkfloExternalTask
