"""Tests for :mod:`siptools_research.database` module."""
import pytest

from siptools_research.database import Database


@pytest.mark.usefixtures('testmongoclient')
def test_update_dataset(config):
    """Test update_dataset method.

    :param config: Configuration file
    """
    database = Database(config)

    # Add some data to a document
    database.update_document(
        'test_id',
        {
            'key1': 'value1',
            'key2': 'value2'
        }
    )
    # Update the document
    updated_document = database.update_document(
        'test_id',
        {
            'key2': 'new_value2',
            'key3': 'value3'
        }
    )

    # Ensure that updated document was saved to db
    assert database.get_document('test_id') == updated_document

    # key1 should not be updated
    assert updated_document['key1'] == 'value1'

    # key2 should have new value
    assert updated_document['key2'] == 'new_value2'

    # key3 should be added to document
    assert updated_document['key3'] == 'value3'


@pytest.mark.parametrize(
    ("search", "document_identifiers"),
    [
        ({}, ['test_id1', 'test_id2', 'test_id3']),
        ({'key1': 'value1'}, ['test_id1', 'test_id2', 'test_id3']),
        ({'key2': 'value2'}, ['test_id1', 'test_id2']),
        ({'key1': 'foo'}, []),
        ({'foo': 'value1'}, []),
    ]
)
@pytest.mark.usefixtures('testmongoclient')
def test_find(config, search, document_identifiers):
    """Test find method.

    :param config: Configuration file
    :param search: Search argument to be used
    :param document_identifiers: Identifiers of documents expected to be
                                 found
    """
    database = Database(config)

    # Add some documents to database
    database.update_document(
        'test_id1',
        {
            'key1': 'value1',
            'key2': 'value2'
        }
    )
    database.update_document(
        'test_id2',
        {
            'key1': 'value1',
            'key2': 'value2'
        }
    )
    database.update_document(
        'test_id3',
        {
            'key1': 'value1',
            'key3': 'value3'
        }
    )

    documents = database.find(search)
    assert [document['_id'] for document in documents] == document_identifiers
