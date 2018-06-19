"""Tests for ``siptools_research.utils.database`` module"""
import uuid
import pytest
import tests.conftest
import siptools_research.utils.database
import pymongo

@pytest.mark.usefixtures('testmongoclient')
def test_add_event():
    """Test add_event function. Adds sample event to empty database and checks
    that new document is created.

    :returns: None
    """

    # Init database client
    database = siptools_research.utils.database.Database(
        tests.conftest.TEST_CONFIG_FILE
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


# TODO: test for set_status()

# TODO: test for add_dataset()
