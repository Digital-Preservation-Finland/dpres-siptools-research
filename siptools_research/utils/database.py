"""Workflow status database interface"""

import datetime
import pymongo
from siptools_research.config import OPTIONS

HOST = OPTIONS['MONGODB_HOST']
DB = OPTIONS['MONGODB_DATABASE']
COLLECTION = OPTIONS['MONGODB_COLLECTION']

# TODO: Initializing mongo client as global variable should allow using the
# same client (same connection) across all modules by importing the
# MONGO_CLIENT variable. For example:
#   MONGO_CLIENT = pymongo.MongoClient(host=HOST)
#   MONGO_COLLECTION = MONGO_CLIENT[DB][COLLECTION]
# However, this will cause some trouble in unit tests, where mongomock is used
# as replacement for real mongodb. Therefore new mongo_client is created in
# each function in this module.

def timestamp():
    """Return time now."""
    return datetime.datetime.utcnow().isoformat()


def add_event(document_id, taskname, result, messages):
    """Add information of workflow task to mongodb.

    :document_id: Mongo document id
    :taskname: Name of the task
    :result: Result string ('failure' or 'success')
    :messages: Information of the event
    :returns: None
    """

    mongo_client = pymongo.MongoClient(host=HOST)
    mongo_collection = mongo_client[DB][COLLECTION]

    mongo_collection.update_one(
        {'_id': document_id},
        {'$set':{'workflow_tasks.' + taskname:{'timestamp': timestamp(),
                                               'messages': messages,
                                               'result': result}}
        },
        upsert=True
    )


def set_status(document_id, status):
    """Add information of workflow task to mongodb.

    :document_id: Mongo document id
    :taskname: Status string
    """
    mongo_client = pymongo.MongoClient(host=HOST)
    mongo_collection = mongo_client[DB][COLLECTION]

    mongo_collection.update_one(
        {'_id': document_id},
        {'$set':{'status': status}},
        upsert=True
    )


def add_dataset(document_id, dataset_id):
    """Add new document

    :document_id: Mongo document id
    :dataset_id: Dataset identifier
    :returns: None
    """
    mongo_client = pymongo.MongoClient(host=HOST)
    mongo_collection = mongo_client[DB][COLLECTION]

    mongo_collection.update_one(
        {'_id': document_id},
        {'$set':{'status': 'Request reveived',
                 'dataset': dataset_id}},
        upsert=True
    )


