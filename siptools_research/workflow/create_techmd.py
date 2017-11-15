# encoding=utf8
"""required tasks to create SIPs from transfers"""

import os
#from siptools_research.luigi.target import TaskFileTarget
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils import database
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.utils.scripts.import_objects import main

class DummyException(Exception):
    """A dummy exception that never raises"""
    pass


class CreateTechnicalMetadata(WorkflowTask):
    """Create PREMIS object files based on SÄHKE2 contents.
    """
    def requires(self):
        """Return required tasks.

        :returns: Files must have been transferred to workspace
        """

        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        """Outputs a task file"""
        return MongoTaskResultTarget(document_id=self.document_id,
                                          taskname=self.task_name)
        #return TaskFileTarget(self.workspace, 'create-technical-metadata')

    def run(self):
        """Creates PREMIS technical metadata files for files in transfer.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None
        """

        try:
            techmd_log = os.path.join(self.workspace, 'logs',
                                      'task-create-technical-metadata.log')
            with open(techmd_log, 'w') as log:
                with redirect_stdout(log):
                    main([self.dataset_id,
                          '--workspace', self.workspace])
                    task_result = 'success'
                    task_messages = "Technical metadata for objects created."

        except DummyException as ex:
            task_result = 'failure'
            task_messages = "Technical metadata for objects could not be "\
                            "created: %s" % ex

        finally:
            if not 'task_result' in locals():
                task_result = 'failure'
                task_messages = "Creation of technical metadata failed due "\
                                "to unknown error."
            database.add_event(self.document_id,
                               self.task_name,
                               task_result,
                               task_messages)
