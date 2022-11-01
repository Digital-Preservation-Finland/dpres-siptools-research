"""Base task classes for the workflow tasks."""
import os

import luigi
from metax_access import (Metax,
                          DS_STATE_INVALID_METADATA,
                          DS_STATE_PACKAGING_FAILED,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)

from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from siptools_research.exceptions import (InvalidContractMetadataError,
                                          InvalidDatasetMetadataError,
                                          InvalidFileMetadataError,
                                          InvalidSIPError,
                                          InvalidDatasetError)


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks.

    In addition to functionality of normal luigi Task, every workflow
    task has some luigi parameters:

    :workspace: Full path to unique self.workspace directory for the
                task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file

    WorkflowTask also has some extra instance variables that can be used
    to identify the task and current workflow, for example when storing
    workflow status information to database:

    :document_id: A unique string that is used for identifying workflows
                  (one mongodb document per workflow) in workflow
                  database. The ``document_id`` is the name (not path)
                  of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
                  workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is
                        created
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Initialize workflow task.

        Calls luigi.Task's __init__ and sets additional instance
        variables.
        """
        super().__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__
        self.sip_creation_path = os.path.join(self.workspace,
                                              'sip-in-progress')


class WorkflowExternalTask(luigi.ExternalTask):
    """Common base class for external workflow tasks.

    External tasks are executed externally from this process and task
    does not implement the run() method, only output() and requires()
    methods. In addition to functionality of normal luigi ExternalTask,
    every task has some luigi parameters:

    :workspace: Full path to unique self.workspace directory for the
                task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file

    WorkflowExternalTask also has some extra instance variables that can
    be used to identify the task and current workflow, forexample when
    storing workflow status information to database:

    :document_id: A unique string that is used for identifying workflows
                  (one mongodb document per workflow) in workflow
                  database. The ``document_id`` is the name (not path)
                  of workspace directory.
    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
                  workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is
                        created
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Initialize external workflow task.

        Calls luigi.Task's __init__ and sets additional instance
        variables.
        """
        super().__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__
        self.sip_creation_path = os.path.join(self.workspace,
                                              'sip-in-progress')


class WorkflowWrapperTask(luigi.WrapperTask):
    """Common base class for all workflow wrapper tasks.

    Wrapper tasks execute other tasks, but does not have implementation
    itself. In addition to functionality of normal luigi WrapperTask,
    every task has three parameters:

    :workspace: Full path to unique self.workspace directory for the
                task.
    :dataset_id: Metax dataset id.
    :config: Path to configuration file
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()


@WorkflowTask.event_handler(luigi.Event.SUCCESS)
def report_task_success(task):
    """Report task success.

    This function is triggered after each WorkflowTask is executed
    succesfully. Adds report of successful task to workflow database.

    :param task: WorkflowTask object
    :returns: ``None``
    """
    database = Database(task.config)
    database.add_task(task.document_id,
                      task.task_name,
                      'success',
                      task.success_message)


@WorkflowTask.event_handler(luigi.Event.FAILURE)
def report_task_failure(task, exception):
    """Report task failure.

    This function is triggered when a WorkflowTask fails. Adds report of
    failed task to workflow database.

    If task failed because dataset is invalid, the preservation status
    of dataset is updated in Metax, and the workflow is disabled.

    :param task: WorkflowTask object
    :param exception: Exception that caused failure
    :returns: ``None``
    """
    database = Database(task.config)
    database.add_task(task.document_id,
                      task.task_name,
                      'failure',
                      f"{task.failure_message}: {str(exception)}")
    config_object = Configuration(task.config)
    metax_client = Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification')
    )

    if isinstance(exception, InvalidDatasetError):
        # Disable workflow
        database.set_disabled(task.document_id)

        # Set preservation status for dataset in Metax
        if isinstance(exception, InvalidSIPError):
            preservation_state \
                = DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
        elif isinstance(exception, (InvalidDatasetMetadataError,
                                    InvalidFileMetadataError,
                                    InvalidContractMetadataError)):
            preservation_state = DS_STATE_INVALID_METADATA
        else:
            preservation_state = DS_STATE_PACKAGING_FAILED
        metax_client.set_preservation_state(
            task.dataset_id,
            preservation_state,
            _get_description(task, exception)
        )


def _get_description(task, exception):
    """Create description for dataset status.

    Max length of the preservation_description attribute in Metax
     is 200 chars.
    """
    system_description = (f"{task.failure_message}: "
                          f"{type(exception).__name__}: {str(exception)}")
    if len(system_description) > 200:
        system_description = system_description[:199]
    return system_description
