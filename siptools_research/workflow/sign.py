"""required tasks to validate SIPs and create AIPs"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_mets import CreateMets

from siptools.scripts.sign_mets import main


class SignSIP(WorkflowTask):
    """Signs METS file.
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires METS file"""
        return {"Create METS":
                CreateMets(workspace=self.workspace,
                           sip_creation_path=self.sip_creation_path,
                           home_path=self.home_path)}

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'sign-sip')

    def run(self):
        """Creates a digital signature file.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """
        #mets_location = os.path.join(self.sip_creation_path, 'mets.xml')
        mets_location = 'mets.xml'
        signature_file = os.path.join(self.sip_creation_path, 'signature.sig')

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.sign-sip')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            sign_log = os.path.join(self.workspace, 'logs',
                                    'task-sign-sip.log')
            save_stdout = sys.stdout
            log = open(sign_log, 'w')
            sys.stdout = log

            main([mets_location, signature_file,
                  '/home/kansallisarkisto/sip_sign_pas.pem'])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': "Digital signature for SIP created."
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace, 'sign-sip'))

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
                outfile.write('Task sign-mets failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)
