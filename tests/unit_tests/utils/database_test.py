"""Tests for :mod:`siptools_research.utils.database` module"""
import uuid
import pytest
import pymongo
import siptools_research.utils.database
import tests.conftest


@pytest.mark.usefixtures('testmongoclient')
def test_add_event():
    """Test add_event function. Adds sample event to empty database and checks
    that new document is created.

    :returns: ``None``
    """

    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Add event for random document
    document_id = uuid.uuid4()
    database.add_event(document_id, 'TestTask', 'success',
                       'Everything went better than expected')

    # Connect to mongodb
    mongoclient = pymongo.MongoClient()
    # Find the document that was modified
    document = mongoclient['siptools-research'].workflow.find_one(
        {'_id': document_id}
    )

    # Check document
    assert document['workflow_tasks']['TestTask']['messages'] == \
        'Everything went better than expected'
    assert document['workflow_tasks']['TestTask']['result'] == 'success'

    # Check that there is no extra documents in database
    assert mongoclient['siptools-research'].workflow.count() == 1


@pytest.mark.usefixtures('testmongoclient')
def test_set_status():
    """Change workflow status twice using ``set_status`` function and check that
    status is updated.

    :returns: ``None``
    """

    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Set status
    database.set_status('test_workflow', 'Original status')

    # Check status
    # pylint: disable=protected-access
    assert database._collection.find_one({'_id': 'test_workflow'})['status'] \
        == 'Original status'

    # Change status
    database.set_status('test_workflow', 'New status')

    # Check that status has changed
    assert database._collection.find_one({'_id': 'test_workflow'})['status'] \
        == 'New status'


@pytest.mark.usefixtures('testmongoclient')
def test_add_workflow():
    """Add new workflow to database using ``add_workflow`` function and check
    that new document contains correct information.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # add new workflow
    database.add_workflow('test_workflow', 'test_dataset')

    # Check result
    # pylint: disable=protected-access
    workflow = database._collection.find_one({'_id': 'test_workflow'})
    workflow['status'] = 'Request received'
    workflow['dataset'] = 'test_dataset'
    workflow['incomplete'] = True


@pytest.mark.usefixtures('testmongoclient')
def test_get_incomplete_datasets():
    """Populates database with few completed, incomplete and disabled datasets
    and checks that ``get_incomplete_workflows`` function returns list of
    incomplete workflows.

    :returns: ``None``
    """
    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    # Populate database
    database.add_workflow('test1', '1')
    database.add_workflow('test2', '2')
    database.set_completed('test2')
    database.add_workflow('test3', '3')
    database.set_disabled('test3')
    database.add_workflow('test4', '4')

    # The workflows found by ``get_incomplete_workflows`` function should be
    # "test1" and "test4"
    workflows = database.get_incomplete_workflows()
    assert set(workflow['_id'] for workflow in workflows) \
        == set(['test1', 'test4'])
