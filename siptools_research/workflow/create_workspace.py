"""Luigi task that creates workspace directory."""

import os
import luigi
from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.utils import database

class CreateWorkspace(WorkflowTask):
    """Creates empty workspace directory."""

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(CreateWorkspace, self).__init__(*args, **kwargs)
        self.task_name = self.__class__.__name__


    def output(self):
        """Outputs workflow_tasks.CreateWorkSpace.result:'success'"""
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Creates workspace directory and adds event information to mongodb.

        :returns: None
        """
        os.mkdir(self.workspace)

        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')
