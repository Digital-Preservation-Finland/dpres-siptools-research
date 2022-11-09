"""Workflow database interface."""

import datetime
import pymongo
from siptools_research.config import Configuration


def _timestamp():
    """Return time now.

    :returns: ISO 8601 string
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


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

    def add_task(self, workflow_id, taskname, result, messages):
        """Add information of workflow task to database.

        :param workflow_id: Workflow identifier i.e. the name of
                            workspace directory
        :param taskname: Name of the task
        :param result: Result string ('failure' or 'success')
        :param messages: Information about the task
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

    def set_enabled(self, workflow_id):
        """Mark workflow enabled.

        :param workflow_id: Workflow identifier
        :returns: ``None``
        """
        self._collection.update_one(
            {
                '_id': workflow_id
            },
            {
                '$set': {
                    'disabled': False
                }
            },
            upsert=True
        )

    def add_workflow(self, workflow_id, dataset_id):
        """Add new workflow.

        The workflow identifier will be the primary key ('_id') of the
        document.

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
                    'status': 'Request received',
                    'dataset': dataset_id,
                    'completed': False,
                    'disabled': False
                }
            },
            upsert=True
        )

    def find(self, search):
        """Search workflows with arbitrary filter.

        :param search: Filter dictionary
        :returns: List of workflows
        """
        return list(self._collection.find(search))

    def get_workflows(self, dataset_id):
        """Get workflows by dataset identifier.

        :param dataset_id: Dataset identifier
        :returns: List of workflows
        """
        return list(self._collection.find({"dataset": dataset_id}))

    def get_one_workflow(self, workflow_id):
        """Get a workflow document by workflow identifier.

        :param workflow_id: Workflow identifier
        :returns: Workflow
        """
        return self._collection.find_one({"_id": workflow_id})

    def get_task_result(self, workflow_id, taskname):
        """Read task result for a workflow.

        :param workflow_id: Workflow identifier
        :param taskname: Name of task
        :returns: Task result
        """
        document = self._collection.find_one({'_id': workflow_id})
        return document['workflow_tasks'][taskname]['result']

    def get_task_timestamp(self, workflow_id, taskname):
        """Read task timestamp for a workflow.

        :param workflow_id: Workflow identifier
        :param taskname: Name of task
        :returns: Task timestamp
        """
        document = self._collection.find_one({'_id': workflow_id})
        if not document:
            raise ValueError

        return document['workflow_tasks'][taskname]['timestamp']

    def get_incomplete_workflows(self):
        """Find all incomplete workflows that are not disabled.

        :returns: List of incomplete workflows
        """
        return list(self._collection.find({'completed': False,
                                           'disabled': False}))
