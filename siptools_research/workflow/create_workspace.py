"""Luigi task that creates workspace directory."""

import os
import luigi
from siptools_research.luigi.task import WorkflowTask

class CreateWorkspace(WorkflowTask):
    """Creates empty workspace directory."""

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(CreateWorkspace, self).__init__(*args, **kwargs)
        self.transfers_path = os.path.join(self.workspace, 'transfers')
        self.aineisto_path = os.path.join(self.transfers_path, 'aineisto')

    def output(self):
        """Outputs task file"""
        return luigi.LocalTarget(self.aineisto_path)

    def run(self):
        """Creates workspace directory and writes dataset id to
        ``transfers/aineisto`` file.

        :returns: None
        """
        os.mkdir(self.workspace)
        os.mkdir(self.transfers_path)
        with open(self.aineisto_path, 'w') as open_file:
            open_file.write(self.dataset_id)
