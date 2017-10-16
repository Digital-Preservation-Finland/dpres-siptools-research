"""Workflow status database interface"""

import datetime
import pymongo

HOST = 'localhost'
DB = 'siptools-research'
COLLECTION = 'workflow'

# Initializing mongo client as global variable should allow using the same
# client (same connection) across all modules by importing the MONGO_CLIENT
# variable.
MONGO_CLIENT = pymongo.MongoClient(host=HOST)
MONGO_COLLECTION = MONGO_CLIENT[DB][COLLECTION]

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

    MONGO_COLLECTION.update_one(
        {'_id': document_id},
        {'$set':{'workflow_tasks.' + taskname:{'timestamp': timestamp(),
                                               'messages': messages,
                                               'result': result}}
        },
        upsert=True
    )


def add_dataset(document_id, dataset_id):
    """Add new document

    :document_id: Mongo document id
    :dataset_id: Dataset identifier
    :returns: None
    """
    MONGO_COLLECTION.update_one(
        {'_id': document_id},
        {'$set':{'status': 'Request reveived',
                 'dataset': dataset_id}},
        upsert=True
    )
