"""Luigi task that creates technical metadata"""
# encoding=utf8

import os
from luigi import LocalTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.utils.scripts.import_objects import main


class CreateTechnicalMetadata(WorkflowTask):
    """Create PREMIS object files.
    """
    success_message = 'Technical metadata for objects created'
    failure_message = 'Technical metadata for objects could not be created'

    def requires(self):
        """Return required tasks.

        :returns: CreateWorkspace task
        """

        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Outputs a task file"""
        return LocalTarget(os.path.join(self.logs_path,
                                        'task-create-technical-metadata.log'))

    def run(self):
        """Creates PREMIS technical metadata files for files in transfer.

        :returns: None
        """

        with self.output().open('w') as log:
            with redirect_stdout(log):
                main([self.dataset_id,
                      '--workspace', self.workspace,
                      '--config', self.config])
