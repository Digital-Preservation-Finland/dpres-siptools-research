"""Base task classes for the workflow tasks."""
from collections import defaultdict

import luigi
from metax_access import (DS_STATE_INVALID_METADATA, DS_STATE_PACKAGING_FAILED,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)

from siptools_research.dataset import Dataset
from siptools_research.exceptions import (BulkInvalidDatasetFileError,
                                          InvalidDatasetError,
                                          InvalidDatasetFileError,
                                          InvalidSIPError)
from siptools_research.models.file_error import FileError


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks.

    In addition to functionality of normal luigi Task, every workflow
    task has some luigi parameters:

    :dataset_id: Dataset identifier.
    :is_target_task: If the task is "target task" the workflow will be
                     disabled when the task has run.
    :config: Path to configuration file

    A WorkflowTask instance also has `Dataset` object that can be
    accessed via `dataset` attribute.
    """

    dataset_id = luigi.Parameter()
    is_target_task = luigi.BoolParameter(default=False)
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Initialize workflow task.

        Calls luigi.Task's __init__ and sets additional instance
        variables.
        """
        super().__init__(*args, **kwargs)
        self.dataset = Dataset(self.dataset_id, config=self.config)


class WorkflowExternalTask(luigi.ExternalTask):
    """Common base class for external workflow tasks.

    External tasks are executed externally from this process and task
    does not implement the run() method, only output() and requires()
    methods. In addition to functionality of normal luigi ExternalTask,
    every task has some luigi parameters:

    :dataset_id: Dataset identifier.
    :config: Path to configuration file

    WorkflowExternalTask also has some extra instance variables that can
    be used to identify the task and current workflow, forexample when
    storing workflow status information to database:

    :sip_creation_path: A path in the workspace in which the SIP is
                        created
    """

    dataset_id = luigi.Parameter()
    config = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Initialize external workflow task.

        Calls luigi.Task's __init__ and sets additional instance
        variables.
        """
        super().__init__(*args, **kwargs)
        self.dataset = Dataset(self.dataset_id, config=self.config)


@WorkflowTask.event_handler(luigi.Event.SUCCESS)
def report_task_success(task):
    """Report task success.

    This function is triggered after each WorkflowTask is executed
    succesfully. Adds report of successful task to workflow database. if
    the Task was the "target task", the workflow is disabled.

    :param task: WorkflowTask object
    :returns: ``None``
    """
    task.dataset.log_task(task.__class__.__name__,
                          'success',
                          task.success_message)

    if task.is_target_task:
        task.dataset.disable()


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
    task.dataset.log_task(task.__class__.__name__,
                          'failure',
                          f"{task.failure_message}: {str(exception)}")

    if isinstance(exception, InvalidDatasetError):

        # Choose new preservation state
        if isinstance(exception, InvalidSIPError):
            new_preservation_state \
                = DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
        elif task.dataset.preservation_state >= 80:
            # TODO: DS_STATE_INVALID_METADATA can not be used if current
            # preservation state is higher than
            # DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION, i.e. when
            # packaging has been started. Therefore,
            # DS_STATE_PACKAGING_FAILED is used even if the failure was
            # caused by invalid metadata. This can be removed if Metax
            # allows DS_STATE_INVALID_METADATA!
            new_preservation_state = DS_STATE_PACKAGING_FAILED
        else:
            # Also invalid or missing files could be reason for failure,
            # but there is no separate preservation state for that
            # situation (TPASPKT-617).
            new_preservation_state = DS_STATE_INVALID_METADATA

        # Set preservation status for dataset in Metax
        task.dataset.set_preservation_state(
            new_preservation_state,
            _get_description(task, exception)
        )

        if isinstance(exception, InvalidDatasetFileError):
            exception = BulkInvalidDatasetFileError(
                message=str(exception),
                file_errors=[exception]
            )

        # Save file-related errors if any were included in the exception
        if isinstance(exception, BulkInvalidDatasetFileError):
            _update_db_errors(task, exception)

        # Disable workflow
        task.dataset.disable()


def _update_db_errors(
        task: WorkflowTask, bulk_exception: BulkInvalidDatasetFileError):
    """
    Update file errors in the database

    :param task: The current workflow task
    :param bulk_exception: Bulk exception containing one or more file error
                           exceptions
    """
    file_id2data = {}
    file_id2non_dataset_errors = defaultdict(list)
    file_id2dataset_errors = defaultdict(list)

    # Collect all errors into two sets:
    # non-dataset related errors and dataset related errors
    for file_error in bulk_exception.file_errors:
        dataset_id = (
            task.dataset.identifier
            if file_error.is_dataset_error else None
        )
        message = str(file_error)

        file_ids = []

        for file in file_error.files:
            file_id = file["id"]

            file_ids.append(file_id)
            file_id2data[file_id] = file

            if dataset_id:
                file_id2dataset_errors[file_id] += [message]
            else:
                file_id2non_dataset_errors[file_id] += [message]

    # Clear existing errors
    if file_id2non_dataset_errors:
        FileError.objects.filter(
            file_id__in=list(file_id2non_dataset_errors.keys()),
            dataset_id=None
        ).delete()

    if file_id2dataset_errors:
        FileError.objects.filter(
            file_id__in=list(file_id2dataset_errors.keys()),
            dataset_id=task.dataset.identifier
        )

    # Insert new errors
    new_file_errors = [
        FileError(
            file_id=file_id,
            storage_identifier=file_id2data[file_id]["storage_identifier"],
            storage_service=file_id2data[file_id]["storage_service"],
            dataset_id=task.dataset.identifier,
            errors=errors
        ) for file_id, errors in file_id2dataset_errors.items()
    ]
    new_file_errors += [
        FileError(
            file_id=file_id,
            storage_identifier=file_id2data[file_id]["storage_identifier"],
            storage_service=file_id2data[file_id]["storage_service"],
            dataset_id=None,
            errors=errors
        ) for file_id, errors in file_id2non_dataset_errors.items()
    ]

    FileError.objects.insert(new_file_errors)


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
