"""Tests for siptools_research.luigi.task module"""

import os
import datetime
import pytest
import luigi.cmdline
import pymongo
import httpretty
from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.task import InvalidDatasetError
from siptools_research.luigi.task import InvalidMetadataError
from siptools_research.config import Configuration


class TestClass(WorkflowTask):
    """Test class that only writes an output file."""
    success_message = 'Test task was successfull'

    def output(self):
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

    def run(self):
        with self.output().open('w') as outputfile:
            outputfile.write('Hello world')


class FailingTestClass(WorkflowTask):
    """Test class that always fails."""
    failure_message = 'An error occurred while running a test task'

    def output(self):
        return luigi.LocalTarget(os.path.join(self.workspace, 'output_file'))

    def run(self):
        raise Exception('Shit hit the fan')


class InvalidDatasetClass(FailingTestClass):
    """Test class that raises InvalidDatasetError"""

    def run(self):
        raise InvalidDatasetError('File validation failed')

class InvalidMetadataClass(FailingTestClass):
    """Test class that raises InvalidDatasetError"""

    def run(self):
        raise InvalidMetadataError('Missing some important metadata')


def test_run_workflowtask(testpath, testmongoclient):
    """Executes TestClass, checks that output file is created, checks that new
    event field is created to mongo document."""

    # Run task like it would be run from command line
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', 'tests.luigi.task_test',
             'TestClass',
             '--workspace', testpath,
             '--dataset-id', '1',
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler',
             '--no-lock')
        )

    # Check that output file is created
    with open(os.path.join(testpath, 'output_file')) as output:
        assert output.read() == 'Hello world'

    # Check that new event is added to workflow database
    conf = Configuration('tests/data/siptools_research.conf')
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()
    # Check 'messages' field
    assert document['workflow_tasks']['TestClass']['messages'] ==\
        'Test task was successfull'
    # Check 'result' field
    assert document['workflow_tasks']['TestClass']['result'] == 'success'
    # Parse the 'timestamp' field to make sure it is correct format
    datetime.datetime.strptime(document['workflow_tasks']['TestClass']\
                               ['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')

    # Check that there is no extra documents in mongo collection
    assert collection.count() == 1


def test_run_failing_task(testpath, testmongoclient):
    """Executes FailingTestClass and checks that report of failed event is
    added to mongo document."""

    # Run task like it would be run from command line
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', 'tests.luigi.task_test',
             'FailingTestClass',
             '--workspace', testpath,
             '--dataset-id', '1',
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler',
             '--no-lock')
        )

    # Check that new event is added to workflow database
    conf = Configuration('tests/data/siptools_research.conf')
    mongoclient = pymongo.MongoClient(host=conf.get('mongodb_host'))
    collection = mongoclient[conf.get('mongodb_database')]\
        [conf.get('mongodb_collection')]
    document = collection.find_one()
    # Check 'messages' field
    assert document['workflow_tasks']['FailingTestClass']['messages'] ==\
        'An error occurred while running a test task: Shit hit the fan'
    # Check 'result' field
    assert document['workflow_tasks']['FailingTestClass']['result'] ==\
        'failure'
    # Parse the 'timestamp' field to make sure it is correct format
    datetime.datetime.strptime(document['workflow_tasks']['FailingTestClass']\
                               ['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')

    # Check that there is no extra documents in mongo collection
    assert collection.count() == 1


def test_invalidDatasetError(testpath, testmongoclient, testmetax):
    """Test that event handler of WorkflowTask correctly deals with
    InvalidDatasetError risen in a task. Event handler should report
    preservation state to Metax.
    """
    # Run task like it would be run from command line
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', 'tests.luigi.task_test',
             'InvalidDatasetClass',
             '--workspace', testpath,
             '--dataset-id', '1',
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler',
             '--no-lock')
        )

    # Check the body of last HTTP request
    assert httpretty.last_request().body \
        == '{"id": "1", "preservation_state": "7"}'
    # Check the method of last HTTP request
    assert httpretty.last_request().method == 'PATCH'


def test_invalidMetadataError(testpath, testmongoclient, testmetax):
    """Test that event handler of WorkflowTask correctly deals with
    InvalidDatasetError risen in a task. Event handler should report
    preservation state to Metax.
    """
    # Run task like it would be run from command line
    with pytest.raises(SystemExit):
        luigi.cmdline.luigi_run(
            ('--module', 'tests.luigi.task_test',
             'InvalidMetadataClass',
             '--workspace', testpath,
             '--dataset-id', '1',
             '--config', 'tests/data/siptools_research.conf',
             '--local-scheduler',
             '--no-lock')
        )

    # Check the body of last HTTP request
    assert httpretty.last_request().body \
        == '{"id": "1", "preservation_state": "7"}'
    # Check the method of last HTTP request
    assert httpretty.last_request().method == 'PATCH'

#TODO: Test for WorkflowWrapperTask

#TODO: Test for WorkfloExternalTask
