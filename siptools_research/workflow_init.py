"""Provides function to start the dataset preservation workflow."""

import os
import uuid
import luigi

from metax_access import Metax, DS_STATE_VALIDATING_METADATA

from siptools_research.config import Configuration
import siptools_research.utils.database
from siptools_research.workflow.cleanup import CleanupWorkspace
from siptools_research.workflow.report_packaging_status \
    import ReportPackagingStatus

TARGET_TASKS = {
    CleanupWorkspace.__name__: CleanupWorkspace,
    ReportPackagingStatus.__name__: ReportPackagingStatus
}


class InitWorkflows(luigi.WrapperTask):
    """A wrapper task that starts/restarts all incomplete workflows."""

    config = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: List of CleanupWorkspace tasks
        """
        packaging_root = Configuration(self.config).get('packaging_root')
        workspace_root = os.path.join(packaging_root, "workspaces")
        database = siptools_research.utils.database.Database(self.config)

        for workflow in database.get_incomplete_workflows():
            workspace = os.path.join(workspace_root, workflow['_id'])

            yield TARGET_TASKS[workflow['target_task']](
                workspace=workspace,
                dataset_id=workflow['dataset'],
                config=self.config
            )


def package_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Package dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    schedule_workflow(dataset_id,
                      ReportPackagingStatus.__name__, config=config)


def preserve_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Preserve dataset.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    schedule_workflow(dataset_id, CleanupWorkspace.__name__, config=config)


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
    conf = Configuration(config)
    metax = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    database = siptools_research.utils.database.Database(config)

    # Get correct version of dataset
    dataset = metax.get_dataset(dataset_id)
    if 'preservation_dataset_version' in dataset:
        # The original dataset has been copied to PAS data catalog.
        # Use the version from PAS data catalog.
        dataset = metax.get_dataset(dataset['preservation_dataset_version']
                                    ['identifier'])

    # Update the dataset identifiers of workflows of original dataset.
    if 'preservation_dataset_origin_version' in dataset:
        original_dataset_identifier \
            = dataset['preservation_dataset_origin_version']['identifier']
        for workflow in database.get_workflows(original_dataset_identifier):
            database.set_dataset(workflow['_id'], dataset['identifier'])

    # Check if enabled workflow already exists
    previous_workflow = None
    # TDDO: implement this in database module?
    for workflow in database.get_workflows(dataset['identifier']):
        if workflow['disabled'] is False:
            if dataset['preservation_state'] < DS_STATE_VALIDATING_METADATA:
                # dataset has been changed, disable previous workflow
                database.set_disabled(workflow['_id'])
            else:
                # found previous enabled workflow
                previous_workflow = workflow
            break

    if previous_workflow:
        # Continue previous workflow
        database.set_target_task(previous_workflow["_id"],
                                 target_task)
    else:
        # Add new workflow to database
        workflow_id = f"aineisto_{dataset_id}-{str(uuid.uuid4())}"
        database.add_workflow(workflow_id, target_task, dataset_id)
