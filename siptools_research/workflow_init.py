"""Provides function to start the dataset preservation workflow."""

import os
import uuid
import subprocess
import luigi
from metax_access import Metax, DS_STATE_IN_PACKAGING_SERVICE

from siptools_research.workflowtask import WorkflowWrapperTask
from siptools_research.config import Configuration
import siptools_research.utils.database
from siptools_research.workflow.cleanup import CleanupWorkspace


class InitWorkflow(WorkflowWrapperTask):
    """A wrapper task that starts workflow by requiring the last task of
    workflow.
    """

    def requires(self):
        """Only returns last task of the workflow.

        :returns: CleanupWorkspace task
        """

        return CleanupWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config)


class InitWorkflows(luigi.WrapperTask):
    """A wrapper task that starts/restarts all incomplete workflows.
    """
    config = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: List of CleanupWorkspace tasks
        """

        packaging_root = Configuration(self.config).get('packaging_root')
        database = siptools_research.utils.database.Database(self.config)

        for workflow in database.get_incomplete_workflows():
            workspace = os.path.join(packaging_root, workflow['_id'])

            yield InitWorkflow(workspace=workspace,
                               dataset_id=workflow['dataset'],
                               config=self.config)


def preserve_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Generates unique id for the workspace and initates packaging workflow.
    Workspace name is used as document id in MongoDB. This function can be
    imported to other python modules.

    :param dataset_id: identifier of dataset
    :param config: path to configuration file
    :returns: ``None``
    """
    # Read configuration file
    conf = Configuration(config)
    packaging_root = conf.get('packaging_root')

    # Set workspace name and path
    workspace_name = "aineisto_%s-%s" % (dataset_id,
                                         str(uuid.uuid4()))
    workspace = os.path.join(packaging_root, workspace_name)

    # Add information to mongodb
    database = siptools_research.utils.database.Database(config)
    database.add_workflow(workspace_name, dataset_id)

    # Report preservation state to Metax
    metax = Metax(
        conf.get('metax_url'),
        conf.get('metax_user'),
        conf.get('metax_password'),
        verify=conf.getboolean('metax_ssl_verification')
    )
    metax.set_preservation_state(dataset_id,
                                 state=DS_STATE_IN_PACKAGING_SERVICE,
                                 system_description='In packaging service')

    # Start luigi workflow. Run in background.
    subprocess.Popen([
        "luigi",
        "--module", "siptools_research.__main__", "InitWorkflow",
        "--dataset-id", dataset_id,
        "--workspace", workspace,
        "--config", config,
        "--logging-conf-file", "/etc/luigi/research_logging.cfg"
    ])
