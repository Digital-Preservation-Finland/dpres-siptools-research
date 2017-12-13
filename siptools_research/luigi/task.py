"""Base task classes for the workflow tasks"""

import os
import luigi
from siptools_research.utils.database import Database
from siptools_research.utils.metax import Metax


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks. In addition to functionality
    of normal luigi Task, every workflow task has some luigi parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file

    WorkflowTask also has some extra instance variables that can be used to
    identify the task and current workflow, forexample when storing workflow
    status information to database:

    :document_id: A unique string that is used for identifying workflows (one
    mongodb document per workflow) in workflow database. The ``document_id`` is
    the name (not path) of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
    workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is created
    :logs_path: A path in the workspace in which the task logs are written
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.Task's __init__ and sets additional instance variables.
        """
        super(WorkflowTask, self).__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__
        self.sip_creation_path = os.path.join(self.workspace,
                                              'sip-in-progress')
        self.logs_path = os.path.join(self.workspace, 'logs')


class WorkflowExternalTask(luigi.ExternalTask):
    """Common base class for all tasks that are executed externally from this
    process and task does not implement the run() method, only output() and
    requires() methods. In addition to functionality of normal luigi
    ExternalTask, every task has some luigi parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file

    WorkflowExternalTask also has some extra instance variables that can be
    used to identify the task and current workflow, forexample when storing
    workflow status information to database:

    :document_id: A unique string that is used for identifying workflows (one
    mongodb document per workflow) in workflow database. The ``document_id`` is
    the name (not path) of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
    workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is created
    :logs_path: A path in the workspace in which the task logs are written
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.Task's __init__ and sets additional instance variables.
        """
        super(WorkflowExternalTask, self).__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__
        self.sip_creation_path = os.path.join(self.workspace,
                                              'sip-in-progress')
        self.logs_path = os.path.join(self.workspace, 'logs')


class WorkflowWrapperTask(luigi.WrapperTask):
    """Common base class for all workflow tasks which execute other tasks, but
    does not have implementation itself.  In addition to functionality of
    normal luigi WrapperTask, every task has two parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()


@WorkflowTask.event_handler(luigi.Event.SUCCESS)
def report_task_success(task):
    """This function is triggered after each WorkflowTask is executed
    succesfully. Adds report of successfull event to workflow database.

    :task: WorkflowTask object
    """
    database = Database(task.config)
    database.add_event(task.document_id,
                       task.task_name,
                       'success',
                       task.success_message)


class InvalidDatasetError(Exception):
    """Exception raised when packaged dataset does not pass validation in
    digital preservation service"""
    pass


class InvalidMetadataError(Exception):
    """Exception raised when SIP can not be created for dataset due to missing
    or invalid metadata.
    """
    pass


@WorkflowTask.event_handler(luigi.Event.FAILURE)
def report_task_failure(task, exception):
    """This function is triggered when a WorkflowTask fails. Adds report of
    successfull event to workflow database.

    :task: WorkflowTask object
    :exception: Exception that caused failure
    """
    database = Database(task.config)
    database.add_event(task.document_id,
                       task.task_name,
                       'failure',
                       "%s: %s" % (task.failure_message, str(exception)))

    if isinstance(exception, InvalidDatasetError):
        metax_client = Metax(task.config)
        metax_client.set_preservation_state(task.dataset_id, '7')
    elif isinstance(exception, InvalidMetadataError):
        metax_client = Metax(task.config)
        metax_client.set_preservation_state(task.dataset_id, '7')
