"""Provides function to start the dataset preservation workflow."""

import os
import uuid
import luigi
from metax_access import (Metax,
                          DS_STATE_VALIDATED_METADATA_UPDATED,
                          DS_STATE_INITIALIZED)

from siptools_research.exceptions import WorkflowExistsError
from siptools_research.config import Configuration
import siptools_research.utils.database
from siptools_research.workflow.cleanup\
    import CleanupFileCache
from siptools_research.workflow.generate_metadata \
    import GenerateMetadata
from siptools_research.workflow.report_dataset_validation_result\
    import ReportDatasetValidationResult

TARGET_TASKS = {
    CleanupFileCache.__name__: CleanupFileCache,
    GenerateMetadata.__name__: GenerateMetadata,
    ReportDatasetValidationResult.__name__: ReportDatasetValidationResult
}


class InitWorkflows(luigi.WrapperTask):
    """A wrapper task that starts/restarts all incomplete workflows."""

    config = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: List of Tasks
        """
        packaging_root = Configuration(self.config).get('packaging_root')
        workspace_root = os.path.join(packaging_root, "workspaces")
        database = siptools_research.utils.database.Database(self.config)

        for workflow in database.get_all_active_workflows():
            workspace = os.path.join(workspace_root, workflow['_id'])

            yield TARGET_TASKS[workflow['target_task']](
                workspace=workspace,
                dataset_id=workflow['dataset'],
                config=self.config
            )


def generate_metadata(dataset_id, config='/etc/siptools_research.conf'):
    """Generate dataset metadata.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    schedule_workflow(dataset_id, GenerateMetadata.__name__, config=config)


def validate_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Validate metadata and files of dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    schedule_workflow(dataset_id,
                      ReportDatasetValidationResult.__name__, config=config)


def preserve_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Preserve dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    schedule_workflow(dataset_id, CleanupFileCache.__name__, config=config)


def schedule_workflow(dataset_id,
                      target_task,
                      config='/etc/siptools_research.conf'):
    """Schedule workflow.

    Generates unique identifier for workflow, and adds workflow to
    database.

    :param dataset_id: identifier of dataset
    :param target_task: Target Task of the workflow
    :param config: path to configuration file
    :returns: ``None``
    """
    database = siptools_research.utils.database.Database(config)
    previous_workflow = database.get_current_workflow(dataset_id)

    if previous_workflow:
        # Check if previous workflow can be continued
        if not previous_workflow['completed']:
            # NOTE: Changing the target task of the workflow while it is
            # still running probably would be OK. But currently  we
            # don't need that kind of functionality, so it is safer to
            # prevent it.
            raise WorkflowExistsError(
                'Active workflow already exists for this dataset.'
            )
        conf = Configuration(config)
        metax = Metax(
            conf.get('metax_url'),
            conf.get('metax_user'),
            conf.get('metax_password'),
            verify=conf.getboolean('metax_ssl_verification')
        )
        dataset = metax.get_dataset(dataset_id)
        if dataset['preservation_state'] in (
            DS_STATE_VALIDATED_METADATA_UPDATED,
            DS_STATE_INITIALIZED
        ):
            # DS_STATE_VALIDATED_METADATA_UPDATED means that dataset
            # metadata has been changed. DS_STATE_INITIALIZED means that
            # user has reset the dataset. User might have edited the
            # dataset when it is in "initialized" state. We don't know
            # what exactly has changed so we have to restart the whole
            # workflow.
            database.set_disabled(previous_workflow['_id'])
            previous_workflow = None

    if previous_workflow:
        # Continue previous workflow
        database.set_target_task(previous_workflow["_id"], target_task)
        database.set_incomplete(previous_workflow["_id"])
    else:
        # Add new workflow to database
        workflow_id = f"aineisto_{dataset_id}-{str(uuid.uuid4())}"
        database.add_workflow(workflow_id, target_task, dataset_id)
