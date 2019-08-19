"""Base task classes for the workflow tasks"""

import os
import luigi
from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from metax_access import Metax, MetaxConnectionError,\
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,\
    DS_STATE_METADATA_VALIDATION_FAILED


class FatalWorkflowError(Exception):
    """Baseclass for errors that make workflow to completion impossible. When
    error of this type is encountered, the task should not rescheduled.
    """
    pass


class InvalidDatasetError(FatalWorkflowError):
    """Exception raised when packaged dataset does not pass validation in
    digital preservation service"""
    pass


class InvalidMetadataError(FatalWorkflowError):
    """Exception raised when SIP can not be created for dataset due to missing
    or invalid metadata.
    """
    pass


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks. In addition to functionality
    of normal luigi Task, every workflow task has some luigi parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file

    WorkflowTask also has some extra instance variables that can be used to
    identify the task and current workflow, for example when storing workflow
    status information to database:

    :document_id: A unique string that is used for identifying workflows (one
       mongodb document per workflow) in workflow database. The ``document_id``
       is the name (not path) of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
       workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is created
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
       mongodb document per workflow) in workflow database. The ``document_id``
       is the name (not path) of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
       workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is created
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


class WorkflowWrapperTask(luigi.WrapperTask):
    """Common base class for all workflow tasks which execute other tasks, but
    does not have implementation itself.  In addition to functionality of
    normal luigi WrapperTask, every task has three parameters:

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

    :param task: WorkflowTask object
    :returns: ``None``
    """
    database = Database(task.config)
    database.add_event(task.document_id,
                       task.task_name,
                       'success',
                       task.success_message)


@WorkflowTask.event_handler(luigi.Event.FAILURE)
def report_task_failure(task, exception):
    """This function is triggered when a WorkflowTask fails. Adds report of
    failed event to workflow database.

    If task failed because of ``InvalidDatasetError``, the preservation status
    of dataset in Metax is updated.

    If task failed because of ``InvalidMetadataError``, the preservation status
    of dataset in Metax is updated.

    :param task: WorkflowTask object
    :param exception: Exception that caused failure
    :returns: ``None``
    """
    database = Database(task.config)
    database.add_event(task.document_id,
                       task.task_name,
                       'failure',
                       "%s: %s" % (task.failure_message, str(exception)))

    if isinstance(exception, FatalWorkflowError):
        # Disable workflow
        database.set_disabled(task.document_id)

    if isinstance(exception, InvalidDatasetError):
        # Set preservation status for dataset in Metax
        config_object = Configuration(task.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        metax_client.set_preservation_state(
            task.dataset_id,
            state=DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
            system_description=_get_description(task, exception)
        )
    elif isinstance(exception, InvalidMetadataError):
        # Set preservation status for dataset in Metax
        config_object = Configuration(task.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        metax_client.set_preservation_state(
            task.dataset_id, state=DS_STATE_METADATA_VALIDATION_FAILED,
            system_description=_get_description(task, exception)
        )


def _get_description(task, exception):
    """Max length of the preservation_description attribute in Metax
     is 200 chars
     """
    system_description = "%s: %s: %s" % (task.failure_message,
                                         type(exception).__name__,
                                         str(exception))
    if len(system_description) > 200:
        system_description = system_description[:199]
    return system_description
