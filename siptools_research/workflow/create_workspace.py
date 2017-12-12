"""Luigi task that creates workspace directory."""

import luigi
from siptools_research.utils import utils
from siptools_research.luigi.task import WorkflowTask

class CreateWorkspace(WorkflowTask):
    """Creates empty workspace directory."""

    success_message = 'Workspace directory create.'
    failure_message = 'Creating workspace directory failed'

    def output(self):
        """Outputs workflow_tasks.CreateWorkSpace.result:'success'"""
        return luigi.LocalTarget(self.logs_path)

    def run(self):
        """Creates workspace directory and adds event information to mongodb.

        :returns: None
        """
        utils.makedirs_exist_ok(self.workspace)
        utils.makedirs_exist_ok(self.sip_creation_path)
        utils.makedirs_exist_ok(self.logs_path)
