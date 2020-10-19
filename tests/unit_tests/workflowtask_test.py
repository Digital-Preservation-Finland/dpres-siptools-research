"""Tests for :mod:`siptools_research.workflowtask` module."""

import os
import datetime

import requests
import luigi.cmdline
import pytest
import pymongo

from metax_access import (
    Metax,
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
    DS_STATE_METADATA_VALIDATION_FAILED
)

import tests.conftest
from siptools_research.workflowtask import WorkflowTask
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.exceptions import InvalidSIPError
from siptools_research.config import Configuration


def run_luigi_task(task_name, workspace):
    """Run a luigi task.

    Run a task like it would be run from command line, using some default
    parameters.

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


class TestTask(WorkflowTask):
    """Test class that only writes an output file."""

    success_message = 'Test task was successfull'

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


class FailingTestTask(WorkflowTask):
    """Test class that always fails."""

    failure_message = 'An error occurred while running a test task'

    def output(self):
        """Create output file.

        :returns: local target: `<workspace>/output_file`
        """
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

    def run(self):
        """Raise exception.

        :returns:  ``None``
        """
        raise Exception('Shit hit the fan')


class InvalidSIPTask(FailingTestTask):
    """Test class that raises InvalidSIPError."""

    def run(self):
        """Raise InvalidSIPError.

        :returns:  ``None``
        """
        raise InvalidSIPError('File validation failed')


class InvalidDatasetMetadataTask(FailingTestTask):
    """Test class that raises InvalidDatasetMetadataError."""

    def run(self):
        """Raise InvalidDatasetMetadataError.

        :returns:  ``None``
        """
        raise InvalidDatasetMetadataError('Missing some important metadata')


class MetaxTask(WorkflowTask):
    """Test class that retrieves dataset from Metax."""

    failure_message = 'Failed retrieving dataste from Metax'

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

    Executes a TestTask, checks that output file is created, checks that new
    event field is created to mongo document.

    :param testpath: temporary directory
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task('TestTask', testpath)

    # Check that output file is created
    with open(os.path.join(testpath, 'output_file')) as output:
        assert output.read() == 'Hello world'

    # Check that new event is added to workflow database
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()
    # Check 'messages' field
    assert document['workflow_tasks']['TestTask']['messages'] ==\
        'Test task was successfull'
    # Check 'result' field
    assert document['workflow_tasks']['TestTask']['result'] == 'success'
    # Parse the 'timestamp' field to make sure it is correct format
    datetime.datetime.strptime(document['workflow_tasks']['TestTask']
                               ['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')

    # Check that there is no extra documents in mongo collection
    assert collection.count() == 1


@pytest.mark.usefixtures('testmongoclient')
def test_run_failing_task(testpath, ):
    """Test running task that fails.

    Executes FailingTestTask and checks that report of failed event is
    added to mongo document.

    :param testpath: temporary directory
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task('FailingTestTask', testpath)

    # Check that new event is added to workflow database
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = (mongoclient[conf.get('mongodb_database')]
                  [conf.get('mongodb_collection')])
    document = collection.find_one()
    # Check 'messages' field
    assert document['workflow_tasks']['FailingTestTask']['messages'] ==\
        'An error occurred while running a test task: Shit hit the fan'
    # Check 'result' field
    assert document['workflow_tasks']['FailingTestTask']['result'] ==\
        'failure'
    # Parse the 'timestamp' field to make sure it is correct format
    datetime.datetime.strptime(document['workflow_tasks']['FailingTestTask']
                               ['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')

    # Check that there is no extra documents in mongo collection
    assert collection.count() == 1


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_invaliddataseterror(testpath, requests_mock):
    """Test that event handler of WorkflowTask.

    Event handler should report preservation state to Metax if
    InvalidSIPError raises in a task.

    :param testpath: temporary directory
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    # Run task like it would be run from command line
    run_luigi_task('InvalidSIPTask', testpath)

    # Check the body of last HTTP request
    request_body = requests_mock.last_request.json()
    assert request_body['preservation_state'] ==\
        DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
    assert request_body['preservation_description'] == 'An error '\
        'occurred while running a test task: InvalidSIPError: '\
        'File validation failed'

    # Check the method of last HTTP request
    assert requests_mock.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient', 'mock_metax_access')
def test_invalidmetadataerror(testpath, requests_mock):
    """Test that event handler of WorkflowTask.

    Event handler should report preservation state to Metax if
    InvalidDatasetMetadataError raises in a task.

    :param testpath: temporary directory
    :param requests_mock: Mocker object
    :returns: ``None``
    """
    requests_mock.patch(
        "https://metaksi/rest/v1/datasets/1"
    )

    # Run task like it would be run from command line
    run_luigi_task('InvalidDatasetMetadataTask', testpath)

    # Check the body of last HTTP request
    request_body = requests_mock.last_request.json()
    assert request_body['preservation_state'] ==\
        DS_STATE_METADATA_VALIDATION_FAILED
    assert request_body['preservation_description'] \
        == ('An error occurred while running a test task: '
            'InvalidDatasetMetadataError: Missing some important metadata')

    # Check the method of last HTTP request
    assert requests_mock.last_request.method == 'PATCH'


@pytest.mark.usefixtures('testmongoclient')
def test_logging(testpath, requests_mock, caplog):
    """Test logging failed HTTP responses."""
    # Create mocked response for HTTP request.
    requests_mock.get('https://metaksi/rest/v1/datasets/1',
                      status_code=403,
                      reason='Access denied',
                      text='No rights to view dataset')

    # Run task that sends HTTP request
    run_luigi_task('MetaxTask', testpath)

    # Check errors in logs
    errors = [r for r in caplog.records if r.levelname == 'ERROR']

    # First error should contain the the body of response to failed request
    assert errors[0].getMessage() == (
        'HTTP request to https://metaksi/rest/v1/datasets/1 failed. '
        'Response from server was: No rights to view dataset'
    )

    # Second logged error should be the raised HTTPError
    assert errors[1].exc_text.startswith('Traceback ')
    exception = errors[1].exc_info[1]
    assert isinstance(exception, requests.HTTPError)
    assert str(exception) == '403 Client Error: Access denied'


# TODO: Test for WorkflowWrapperTask

# TODO: Test for WorkfloExternalTask
