"""Tests for :mod:`siptools_research.workflowtask` module."""

import datetime
import os

import luigi.cmdline
import pytest
import requests
from metax_access import (DS_STATE_INVALID_METADATA,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
                          Metax)

import tests.conftest
from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from siptools_research.exceptions import (InvalidDatasetMetadataError,
                                          InvalidSIPError)
from siptools_research.workflowtask import WorkflowTask


def run_luigi_task(task_name, workspace):
    """Run a luigi task.

    Run a task like it would be run from command line, using some
    default parameters.

    :task (str): Name of task to be executed
    :workspace: Workspace directory for task
    :returns: ``None``
    """
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', 'tests.unit_tests.workflowtask_test',
             task_name,
             '--workspace', workspace,
             '--dataset-id', '1',
             '--config', tests.conftest.UNIT_TEST_CONFIG_FILE,
             '--local-scheduler',
             '--no-lock')
        )


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

        :returns: local target: `<workspace>/output_file`
        """
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

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


class InvalidSIPTask(FailingTask):
    """Test class that raises InvalidSIPError."""

    def run(self):
        """Raise InvalidSIPError.

        :returns:  ``None``
        """
        raise InvalidSIPError('File validation failed')


class InvalidDatasetMetadataTask(FailingTask):
    """Test class that raises InvalidDatasetMetadataError."""

    def run(self):
        """Raise InvalidDatasetMetadataError.

        :returns:  ``None``
        """
        raise InvalidDatasetMetadataError('Missing some important metadata')


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
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        metax_client.get_dataset('1')


# pylint: disable=unused-argument
@pytest.mark.usefixtures('mock_luigi_config_path', 'testmongoclient')
def test_run_workflowtask(testpath):
    """Test WorkflowTask execution.

    Executes a DummyTask, checks that output file is created, checks
    that new event field is created to mongo document.

    :param testpath: temporary directory
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task('DummyTask', str(testpath))

    # Check that output file is created
    assert (testpath / "output_file").read_text() == "Hello world"

    # Check that new event is added to workflow database
    database = Database(tests.conftest.UNIT_TEST_CONFIG_FILE)
    workflow = database.get_one_workflow(testpath.name)
    # Check 'messages' field
    assert workflow['workflow_tasks']['DummyTask']['messages'] \
        == 'Test task was successful'
    # Check 'result' field
    assert workflow['workflow_tasks']['DummyTask']['result'] == 'success'
    # Parse the 'timestamp' field to make sure it is correct format
    timestamp = workflow['workflow_tasks']['DummyTask']['timestamp']
    assert timestamp.endswith("+00:00")
    assert datetime.datetime.strptime(
        timestamp[:-6],  # Remove the UTC offset
        '%Y-%m-%dT%H:%M:%S.%f'
    )

    # Check that there is no extra workflows in database
    assert len(database.find(None)) == 1


@pytest.mark.usefixtures('testmongoclient')
def test_run_failing_task(testpath, ):
    """Test running task that fails.

    Executes FailingTask and checks that report of failed event is added
    to mongo document.

    :param testpath: temporary directory
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task('FailingTask', str(testpath))

    # Check that new event is added to workflow database
    database = Database(tests.conftest.UNIT_TEST_CONFIG_FILE)
    workflow = database.get_one_workflow(testpath.name)
    # Check 'messages' field
    assert workflow['workflow_tasks']['FailingTask']['messages'] \
        == 'An error occurred while running a test task: Shit hit the fan'
    # Check 'result' field
    assert workflow['workflow_tasks']['FailingTask']['result'] \
        == 'failure'
    # Parse the 'timestamp' field to make sure it is correct format
    timestamp = workflow['workflow_tasks']['FailingTask']['timestamp']
    assert timestamp.endswith("+00:00")
    datetime.datetime.strptime(
        timestamp[:-6],  # Remove the UTC offset
        '%Y-%m-%dT%H:%M:%S.%f'
    )

    # Check that there is no extra workflows in database
    assert len(database.find(None)) == 1


@pytest.mark.parametrize(
    ('task', 'expected_state', 'expected_description'),
    (
        [
            'InvalidSIPTask',
            DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
            'An error occurred while running a test task: '
            'InvalidSIPError: File validation failed'
        ],
        [
            'InvalidDatasetMetadataTask',
            DS_STATE_INVALID_METADATA,
            'An error occurred while running a test task: '
            'InvalidDatasetMetadataError: Missing some important metadata'
        ],
    )
)
@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_invalid_dataset_error(testpath, requests_mock, task, expected_state,
                               expected_description):
    """Test event handler of WorkflowTask.

    Event handler should report preservation state to Metax if
    InvalidDatasetError raises in a task.

    :param testpath: temporary directory
    :param requests_mock: Mocker object
    :param task: Test task to be run
    :param expected_state: Preservation state that should be reported to
                           Metax
    :param expected_description: Preservation description that should
                                 be reported to Metax
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task(task, str(testpath))

    # Check the body of last HTTP request
    request_body = requests_mock.last_request.json()
    assert request_body['preservation_state'] == expected_state
    assert request_body['preservation_description'] == expected_description

    # Check the method of last HTTP request
    assert requests_mock.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient')
def test_logging(testpath, requests_mock, caplog):
    """Test logging failed HTTP responses."""
    # Create mocked response for HTTP request.
    requests_mock.get('https://metaksi/rest/v2/datasets/1',
                      status_code=403,
                      reason='Access denied',
                      text='No rights to view dataset')

    # Run task that sends HTTP request
    run_luigi_task('MetaxTask', str(testpath))

    # Check errors in logs
    errors = [r for r in caplog.records if r.levelname == 'ERROR']

    # First error should contain the the body of response to failed
    # request
    assert errors[0].getMessage() == (
        'HTTP request to https://metaksi/rest/v2/datasets/1?'
        'include_user_metadata=true failed. Response from server was: '
        'No rights to view dataset'
    )

    # Second logged error should be the raised HTTPError
    assert errors[1].exc_text.startswith('Traceback ')
    exception = errors[1].exc_info[1]
    assert isinstance(exception, requests.HTTPError)
    assert str(exception).startswith('403 Client Error: Access denied')


# TODO: Test for WorkflowWrapperTask

# TODO: Test for WorkfloExternalTask
