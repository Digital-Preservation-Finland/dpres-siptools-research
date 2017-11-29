"""Workflow status database interface"""

import datetime
import pymongo
from siptools_research.config import Configuration

def timestamp():
    """Return time now."""
    return datetime.datetime.utcnow().isoformat()


class Database(object):
    """Workflow status database"""

    _collection = None
    client = None

    def __init__(self, config_file):

        if self._collection is None:
            conf = Configuration(config_file)
            self.client = pymongo.MongoClient(host=conf.get("mongodb_host"),
                                              port=27017)
            self._collection = self.client[conf.get("mongodb_database")]\
                               [conf.get("mongodb_collection")]


    def add_event(self, document_id, taskname, result, messages):
        """Add information of workflow task to mongodb.

        :document_id: Mongo document id
        :taskname: Name of the task
        :result: Result string ('failure' or 'success')
        :messages: Information of the event
        :returns: None
        """

        self._collection.update_one(
            {'_id': document_id},
            {'$set':{'workflow_tasks.' + taskname:{'timestamp': timestamp(),
                                                   'messages': messages,
                                                   'result': result}}
            },
            upsert=True
        )


    def set_status(self, document_id, status):
        """Add information of workflow task to mongodb.

        :document_id: Mongo document id
        :taskname: Status string
        """

        self._collection.update_one(
            {'_id': document_id},
            {'$set':{'status': status}},
            upsert=True
        )


    def add_dataset(self, document_id, dataset_id):
        """Add new document

        :document_id: Mongo document id
        :dataset_id: Dataset identifier
        :returns: None
        """

        self._collection.update_one(
            {'_id': document_id},
            {'$set':{'status': 'Request reveived',
                     'dataset': dataset_id}},
            upsert=True
        )
