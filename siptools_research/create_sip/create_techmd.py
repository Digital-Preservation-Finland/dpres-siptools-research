# encoding=utf8
"""required tasks to create SIPs from transfers"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils import touch_file

from siptools_research.workflow.task import WorkflowTask, WorkflowExternalTask

from siptools_research.create_sip.virus import ScanVirus

from siptools_research.scripts.import_objects_sahke2 import main


class CreateTechnicalMetadata(WorkflowTask):
    """Create PREMIS object files based on SÄHKE2 contents.
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires completed virus check"""
        return {"Virus scan":
                ScanVirus(workspace=self.workspace,
                          sip_creation_path=self.sip_creation_path,
                          home_path=self.home_path)}

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
        s2_location = os.path.join(self.sip_creation_path, 'sahke2.xml')
        print 'create_techmd.s2_location %s' % s2_location

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-technical-metadata')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            techmd_log = os.path.join(self.workspace, 'logs',
                                      'task-create-technical-metadata.log')
            save_stdout = sys.stdout
            log = open(techmd_log, 'w')
            sys.stdout = log

            main([self.sip_creation_path,
                  '--sahke2', 'sahke2.xml',
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

            failed_log = FailureLog(self.workspace).output()
            with failed_log.open('w') as outfile:
                outfile.write('Task create-techmd failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)


class TechMDComplete(WorkflowExternalTask):
    """Task that completes after techMD-files have been created.
    """

    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'create-technical-metadata')
