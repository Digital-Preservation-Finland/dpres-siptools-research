"""Luigi task that creates workspace directory."""

from siptools_research.utils import utils
from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.target import MongoTaskResultTarget
import siptools_research.utils.database

class CreateWorkspace(WorkflowTask):
    """Creates empty workspace directory."""

    def output(self):
        """Outputs workflow_tasks.CreateWorkSpace.result:'success'"""
        return MongoTaskResultTarget(self.document_id, self.task_name,
                                     self.config)

    def run(self):
        """Creates workspace directory and adds event information to mongodb.

        :returns: None
        """
        utils.makedirs_exist_ok(self.workspace)
        utils.makedirs_exist_ok(self.sip_creation_path)
        utils.makedirs_exist_ok(self.logs_path)

        database = siptools_research.utils.database.Database(self.config)
        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')
