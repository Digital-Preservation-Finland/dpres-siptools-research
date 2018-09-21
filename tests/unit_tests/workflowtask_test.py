"""Tests for siptools_research.workflowtask module"""

import os
import json
import datetime
import pytest
import tests.conftest
import luigi.cmdline
import pymongo
import httpretty
import mock

from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflowtask import InvalidDatasetError
from siptools_research.workflowtask import InvalidMetadataError
from siptools_research.config import Configuration
from metax_access import MetaxConnectionError,\
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,\
    DS_STATE_METADATA_VALIDATION_FAILED


def run_luigi_task(task_name, workspace):
    """Run a luigi task (using some default parameters) like it would be run
    from command line.

    :task (str): Name of task to be executed
    :workspace: Workspace directory for task
    :returns: None
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
        """Creates output file"""
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

    def run(self):
        """Writes something to output file"""
        with self.output().open('w') as outputfile:
            outputfile.write('Hello world')


class FailingTestTask(WorkflowTask):
    """Test class that always fails."""
    failure_message = 'An error occurred while running a test task'

    def output(self):
        """Creates output file"""
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

    def run(self):
        """Raises exception"""
        raise Exception('Shit hit the fan')


class InvalidDatasetTask(FailingTestTask):
    """Test class that raises InvalidDatasetError"""

    def run(self):
        raise InvalidDatasetError('File validation failed')


class InvalidMetadataTask(FailingTestTask):
    """Test class that raises InvalidDatasetError"""

    def run(self):
        raise InvalidMetadataError('Missing some important metadata')


class MetaxConnectionErrorTask(FailingTestTask):
    """Test class that raises InvalidDatasetError"""

    def run(self):
        raise MetaxConnectionError


# pylint: disable=unused-argument
@pytest.mark.usefixtures('mock_luigi_config_path')
def test_run_workflowtask(testpath, testmongoclient):
    """Executes TestTask, checks that output file is created, checks that new
    event field is created to mongo document."""

    # Run task like it would be run from command line
    run_luigi_task('TestTask', testpath)

    # Check that output file is created
    with open(os.path.join(testpath, 'output_file')) as output:
        assert output.read() == 'Hello world'

    # Check that new event is added to workflow database
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
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


def test_run_failing_task(testpath, testmongoclient):
    """Executes FailingTestTask and checks that report of failed event is
    added to mongo document."""

    # Run task like it would be run from command line
    run_luigi_task('FailingTestTask', testpath)

    # Check that new event is added to workflow database
    conf = Configuration(tests.conftest.UNIT_TEST_CONFIG_FILE)
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
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


def test_invaliddataseterror(testpath, testmongoclient, testmetax):
    """Test that event handler of WorkflowTask correctly deals with
    InvalidDatasetError risen in a task. Event handler should report
    preservation state to Metax.
    """
    # Run task like it would be run from command line
    run_luigi_task('InvalidDatasetTask', testpath)

    # Check the body of last HTTP request
    request_body = json.loads(httpretty.last_request().body)
    assert request_body['preservation_state'] ==\
        DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
    assert request_body['preservation_description'] == 'An error '\
        'occurred while running a test task: InvalidDatasetError: '\
        'File validation failed'

    # Check the method of last HTTP request
    assert httpretty.last_request().method == 'PATCH'


def test_invalidmetadataerror(testpath, testmongoclient, testmetax):
    """Test that event handler of WorkflowTask correctly deals with
    InvalidDatasetError risen in a task. Event handler should report
    preservation state to Metax.
    """
    # Run task like it would be run from command line
    run_luigi_task('InvalidMetadataTask', testpath)

    # Check the body of last HTTP request
    request_body = json.loads(httpretty.last_request().body)
    assert request_body['preservation_state'] ==\
        DS_STATE_METADATA_VALIDATION_FAILED
    assert request_body['preservation_description'] == 'An error '\
        'occurred while running a test task: Missing some important metadata'

    # Check the method of last HTTP request
    assert httpretty.last_request().method == 'PATCH'


def test_metaxconnectionerror(testpath, testmongoclient, testmetax):
    """Test that event handler of WorkflowTask correctly deals with
    MetaxConnectionError risen in a task. Event handler should send
    an email to configured address.
    """

    with mock.patch('siptools_research.workflowtask.mail.send') \
            as mock_sendmail:
        # Run task like it would be run from command line
        run_luigi_task('MetaxConnectionErrorTask', testpath)
        mock_sendmail.assert_called_once_with('test.sender@tpas.fi',
                                              'tpas.admin@csc.fi',
                                              str(MetaxConnectionError()),
                                              str(MetaxConnectionError()))
        # Check the body of last HTTP request
        assert httpretty.last_request().method == 'PATCH'


# TODO: Test for WorkflowWrapperTask

# TODO: Test for WorkfloExternalTask
