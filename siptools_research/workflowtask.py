"""Base task classes for the workflow tasks."""
import os
from pathlib import Path
import shutil

import luigi
from metax_access import (Metax,
                          DS_STATE_INVALID_METADATA,
                          DS_STATE_PACKAGING_FAILED,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)
from requests import HTTPError

from siptools_research.config import Configuration
from siptools_research.utils.database import Database
from siptools_research.exceptions import InvalidSIPError, InvalidDatasetError


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

    :workflow_id: Identifier of the workflow. Generated from the name of
                  workspace, which should be unique
    :sip_creation_path: A path in the workspace in which the SIP is
                        created
    """

    # TODO: maybe workspace parameter could be removed?
    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Initialize workflow task.

        Calls luigi.Task's __init__ and sets additional instance
        variables.
        """
        super().__init__(*args, **kwargs)
        self.workflow_id = os.path.basename(self.workspace)
        _workspace = Path(self.workspace)
        self.metadata_generation_workspace = _workspace / "metadata_generation"
        self.validation_workspace = _workspace / "validation"
        self.preservation_workspace = _workspace / "preservation"
        self.sip_creation_path \
            = self.preservation_workspace / "sip-in-progress"

    def get_metax_client(self):
        """Initialize Metax client."""
        config_object = Configuration(self.config)
        return Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )


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

    :workflow_id: Identifier of the workflow. Generated from the name of
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
        self.workflow_id = os.path.basename(self.workspace)
        self.sip_creation_path = os.path.join(self.workspace,
                                              'sip-in-progress')


@WorkflowTask.event_handler(luigi.Event.SUCCESS)
def report_task_success(task):
    """Report task success.

    This function is triggered after each WorkflowTask is executed
    succesfully. Adds report of successful task to workflow database. if
    the Task was the target Task, the workflow is marked completed and
    the workspace is removed.

    :param task: WorkflowTask object
    :returns: ``None``
    """
    database = Database(task.config)
    database.add_task(task.workflow_id,
                      task.__class__.__name__,
                      'success',
                      task.success_message)

    if task.__class__.__name__ \
            == database.get_one_workflow(task.workflow_id)['target_task']:
        database.set_completed(task.workflow_id)
        shutil.rmtree(task.workspace)


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
    database.add_task(task.workflow_id,
                      task.__class__.__name__,
                      'failure',
                      f"{task.failure_message}: {str(exception)}")

    if isinstance(exception, InvalidDatasetError):
        # Set preservation status for dataset in Metax
        if isinstance(exception, InvalidSIPError):
            preservation_state \
                = DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
        else:
            # Also invalid or missing files could be reason for failure,
            # but there is no separate preservation state for that
            # situation.
            preservation_state = DS_STATE_INVALID_METADATA
        try:
            task.get_metax_client().set_preservation_state(
                task.dataset_id,
                preservation_state,
                _get_description(task, exception)
            )
        except HTTPError:
            # TODO: DS_STATE_INVALID_METADATA can not be used if current
            # preservation state is higher than
            # DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION, i.e. when
            # packaging has been started. Therefore,
            # DS_STATE_PACKAGING_FAILED is used even if the failure was
            # caused by invalid metadata. This can be removed if Metax
            # allows DS_STATE_INVALID_METADATA!
            task.get_metax_client().set_preservation_state(
                task.dataset_id,
                DS_STATE_PACKAGING_FAILED,
                _get_description(task, exception)
            )

        # Disable workflow
        database.set_disabled(task.workflow_id)


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
