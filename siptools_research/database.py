"""Workflow database interface."""

import pymongo
from mongoengine import register_connection

from siptools_research.config import Configuration


def connect_mongoengine(config_path):
    config = Configuration(config_path)
    mongodb_host = config.get("mongodb_host")

    register_connection(
        host=f"mongodb://{mongodb_host}:27017/siptools-research",
        tz_aware=True,
        # Connect on first operation instead of instantly.
        # This is to prevent MongoClient from being created before a fork,
        # which leads to unexpected behavior.
        connect=False,
        alias="siptools_research"
    )


# TODO: We should migrate this to use MongoEngine instead. This makes
# DB access consistent and also documents the structure, which currently
# has to be deduced from the places where this class is used.
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
