"""Workflow database interface."""

import pymongo
from siptools_research.config import Configuration


class Database:
    """Workflow status database."""

    _collection = None
    _client = None

    def __init__(self, config_file):
        """Initialize new pymongo client.

        :param config_file: path to configurtion file
        """
        if self._collection is None:
            conf = Configuration(config_file)
            self._client = pymongo.MongoClient(host=conf.get("mongodb_host"),
                                               port=27017)
            self._collection = (
                self._client[conf.get("mongodb_database")]
                [conf.get("mongodb_collection")]
            )

    def update_document(self, identifier, document):
        """Update workflow document.

        :param identifier: Document identifier
        :param document: Dictionary of fields to be updated
        :returns: Updated document
        """
        result = self._collection.find_one_and_update(
            {
                '_id': identifier
            },
            {
                '$set': document
            },
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER
        )

        return result

    def find(self, search):
        """Search workflow documents with arbitrary filter.

        :param search: Filter dictionary
        :returns: List of workflow documents
        """
        return list(self._collection.find(search))

    def get_document(self, identifier):
        """Get a workflow document by dataset identifier.

        :param dataset_id: Dataset identifier
        :returns: Workflow document
        """
        return self._collection.find_one({"_id": identifier})
