"""required tasks to validate SIPs and create AIPs"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils import touch_file

from siptools_research.workflow.task import WorkflowTask

from siptools_research.create_sip.sign import SignSIP

from siptools.scripts.compress import main


class CompressSIP(WorkflowTask):
    """Compresses contents to a tar file as SIP.

    :path: workspace/created_sip
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    sip_output_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires signature file"""
        return {"Sign SIP":
                SignSIP(workspace=self.workspace,
                        sip_creation_path=self.sip_creation_path,
                        home_path=self.home_path)}

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'compress-sip')

    def run(self):
        """Creates a tar archive file conatining mets.xml,
        signature.sig, sahke2.xml and all content files.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """
        if not os.path.isdir(self.sip_output_path):
            os.makedirs(self.sip_output_path)

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.compress-sip')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        sip_name = os.path.join(self.sip_output_path,
                                (os.path.basename(self.workspace) + '.tar'))

        try:
            compress_log = os.path.join(self.workspace, 'logs',
                                        'task-compress-sip.log')
            save_stdout = sys.stdout
            log = open(compress_log, 'w')
            sys.stdout = log

            main(['--tar_filename', sip_name, self.sip_creation_path])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': ("Transfer files and metadata compressed into "
                             "SIP archive file.")
            }
            mongo_task.write(task_result)
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
            mongo_status.write('SIP created')

            # task output
            touch_file(TaskFileTarget(self.workspace, 'compress-sip'))

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
                outfile.write('Task compress-sip failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)
