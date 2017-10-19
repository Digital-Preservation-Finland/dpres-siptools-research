# encoding=utf8
"""required tasks to create SIPs from transfers"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import touch_file

from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask

from siptools_research.workflow.create_workspace import CreateWorkspace

from siptools_research.utils.scripts.import_objects import main


class CreateTechnicalMetadata(WorkflowTask):
    """Create PREMIS object files based on SÄHKE2 contents.
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Return required tasks.-1

        :returns: Files must have been transferred to workspace
        """

        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-technical-metadata')

    def run(self):
        """Creates PREMIS technical metadata files for files in transfer.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-technical-metadata')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            with open(os.path.join(self.workspace, 'transfers', 'aineisto')) as infile:
                dataset_id = infile.read()

            techmd_log = os.path.join(self.workspace, 'logs',
                                      'task-create-technical-metadata.log')
            save_stdout = sys.stdout
            log = open(techmd_log, 'w')
            sys.stdout = log

            main([dataset_id,
                  '--workspace', self.sip_creation_path])
            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': "Technical metadata for objects created."
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace,
                                      'create-technical-metadata'))

        except Exception as ex:
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'failure',
                'messages': traceback.format_exc()
            }
            mongo_status.write('rejected')
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
            mongo_task.write(task_result)
