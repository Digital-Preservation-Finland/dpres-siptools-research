"""Luigi task that gets files from Ida."""

import os
from luigi import Parameter
from siptools_research.utils import ida
from siptools_research.utils import database
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace


class GetFiles(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """

    dataset_id = Parameter()
    workspace = Parameter()

    def requires(self):
        """Requires create dmdSec file task"""
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Gets files from Ida.

        :returns: None
        """

        # TODO: Hard coded file id and filepath for testing
        os.mkdir(os.path.join(self.workspace, 'files'))
        filepath = os.path.join(self.workspace, 'files', 'file_1')
        ida.download_file('pid:urn:1', filepath)

        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')
