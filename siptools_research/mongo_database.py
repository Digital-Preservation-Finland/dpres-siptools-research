"""Writes data to MongoDB"""

import datetime

from siptools_research.target import MongoDBTarget


TIMESTAMP = datetime.datetime.utcnow().isoformat()


def create_mongodb_document(document_id, username, transfer_name):
    """Creates a document in MongoDB, writes data"""

    mongo_transfer_name = MongoDBTarget(document_id, 'transfer_name')
    mongo_timestamp = MongoDBTarget(document_id, 'timestamp')
    mongo_username = MongoDBTarget(document_id, 'username')

    mongo_transfer_name.write(transfer_name)
    mongo_timestamp.write(TIMESTAMP)
    mongo_username.write(username)

    return 0


def write_document_status(document_id, status):
    """Writes a status to the document and updates the timestamp."""

    mongo_status = MongoDBTarget(document_id, 'status')
    mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

    mongo_status.write(status)
    mongo_timestamp.write(TIMESTAMP)

    return 0


def write_mets_objid(document_id, mets_objid):
    """Writes the mets_objid for a MongoDB document"""

    mongo_mets_objid = MongoDBTarget(document_id, 'mets_objid')

    mongo_mets_objid.write(mets_objid)

    return 0


def write_mongodb_task(document_id, task_name, result, message):
    """Appends data about workflow tasks to the wf_task element"""

    mongo_task = MongoDBTarget(document_id, "wf_tasks.%s" % task_name)

    task_result = {
        'timestamp': TIMESTAMP,
        'result': result,
        'messages': message
    }

    mongo_task.write(task_result)

    return 0
