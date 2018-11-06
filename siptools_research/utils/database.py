"""Workflow database interface"""

import datetime
import pymongo
from siptools_research.config import Configuration


def _timestamp():
    """Return time now.

    :returns: Time stamp string
    """
    return datetime.datetime.utcnow().isoformat()


class Database(object):
    """Workflow status database"""

    _collection = None
    client = None

    def __init__(self, config_file):
        """Initialize new pymongo client if it does not exist already
        (singleton design pattern)

        :param config_file: path to configurtion file
        """

        if self._collection is None:
            conf = Configuration(config_file)
            self.client = pymongo.MongoClient(host=conf.get("mongodb_host"),
                                              port=27017)
            self._collection = (
                self.client[conf.get("mongodb_database")]
                [conf.get("mongodb_collection")]
            )

    def add_event(self, workflow_id, taskname, result, messages):
        """Add information of workflow task to mongodb.

        :param workflow_id: Workflow identifier i.e. the name of workspace
            directory
        :param taskname: Name of the task
        :param result: Result string ('failure' or 'success')
        :param messages: Information of the event
        :returns: ``None``
        """

        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'workflow_tasks.' + taskname: {
                        'timestamp': _timestamp(),
                        'messages': messages,
                        'result': result
                    }
                }
            },
            upsert=True
        )

    def set_status(self, workflow_id, status):
        """Set the status string for a workflow.

        :param workflow_id: Workflow identifier
        :param status: Status string
        :returns: ``None``
        """

        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'status': status
                }
            },
            upsert=True
        )

    def set_completed(self, workflow_id):
        """Mark workflow completed.

        :param workflow_id: Workflow identifier
        :returns: ``None``
        """

        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'completed': True
                }
            },
            upsert=True
        )

    def set_disabled(self, workflow_id):
        """Mark workflow disabled.

        :param workflow_id: Workflow identifier
        :returns: ``None``
        """

        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'disabled': True
                }
            },
            upsert=True
        )

    def add_workflow(self, workflow_id, dataset_id):
        """Add new workflow. The workflow identity will be the primary key
        ('_id') of the document.

        :param workflow_id: Workflow identifier, i.e. the name of workspace
            directory
        :param dataset_id: Dataset identifier
        :returns: ``None``
        """

        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'status': 'Request reveived',
                    'dataset': dataset_id,
                    'completed': False,
                    'disabled': False
                }
            },
            upsert=True
        )

    def get_event_result(self, workflow_id, taskname):
        """Read event result for a workflow.

        :param workflow_id: Mongo workflow id
        :param taskname: Name of task
        :returns: ``None``
        """
        document = self._collection.find_one({'_id': workflow_id})
        return document['workflow_tasks'][taskname]['result']

    def get_incomplete_workflows(self):
        """Find all incomplete workflows that are not disabled

        :returns: List of incomplete workflows
        """
        return list(self._collection.find({'completed': False,
                                           'disabled': False}))
