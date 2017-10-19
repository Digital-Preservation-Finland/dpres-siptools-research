# encoding=utf8
"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback

from datetime import datetime
from luigi import Parameter

from siptools.scripts import compile_structmap

from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget, TaskLogTarget

from siptools_research.utils.utils import touch_file
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata



class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.
    """
    workspace = Parameter()

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                CreateDescriptiveMetadata(workspace=self.workspace,
                                          dataset_id=self.dataset_id),
                "Create provenance information completed":
                CreateProvenanceInformation(workspace=self.workspace,
                                            dataset_id=self.dataset_id),
                "Create technical metadata completed":
                CreateTechnicalMetadata(workspace=self.workspace,
                                        dataset_id=self.dataset_id)}

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-structmap')

    def run(self):

        """Creates a METS structural map file based on a folder structure. Top folder is given as a parameter.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   ('wf_tasks.create-structural-map'
                                    '-and-file-section'))
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')
        task_result = None

        structmap_log = TaskLogTarget(self.workspace,
                                     'create-struct-map')
        # Redirect stdout to logfile
        with structmap_log.open('w') as log:
            with redirect_stdout(log):

                try:

                    workspace = self.workspace
                    compile_structmap.main([
                            '--workspace', workspace])

                    task_result = 'success'
                    task_messages = "Structrural map and filesec created."

                    mongo_task.write(task_result)

                    # task output
                    touch_file(TaskFileTarget(self.workspace, 'create-structmap'))

                except KeyError as ex:
                   task_result = 'failure'
                   task_messages = 'Could not create structrural map, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message

                   mongo_status.write('rejected')


                finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Creation of structmap and filesec "\
                                        "failed due to unknown error."

                    mongo_timestamp.write(
                        datetime.utcnow().isoformat()
                   )

                    mongo_task.write(
                    {
                     'timestamp': datetime.utcnow().isoformat(),
                     'result': task_result,
                     'messages': task_messages
                    }
                    )

