"""Luigi wrapper task that starts workflow for a given dataset.

To start the workflow for dataset 1234 (for example), run luigi from
commandline::

   luigi --module siptools_research.workflow.init_workflow InitWorkflow
   --workspace-root /var/spool/siptools-research --dataset-id 1234
"""

import os
import luigi
from siptools_research.workflow.create_workspace import CreateWorkspace

class InitWorkflow(luigi.WrapperTask):
    """A wrapper task that starts workflow by requiring the last task of
    workflow.
    """

    workspace_root = luigi.Parameter()
    dataset_id = luigi.Parameter()

    def requires(self):
        """Generates name for a workspace and returns last task of the
        workflow.

        :returns: None
        """

        workspace_name = "aineisto_%s" % (self.dataset_id)
        workspace = os.path.join(self.workspace_root, workspace_name)

        # TODO: For testing purposes the task is NOT the last task, but the
        # last IMPLEMENTED task
        return CreateWorkspace(workspace=workspace,
                               dataset_id=self.dataset_id)
