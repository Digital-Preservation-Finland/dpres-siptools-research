"""
Digital signature validation.

"""

import os
import datetime

from uuid import uuid4

from luigi import Parameter

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget,\
        TaskLogTarget
from siptools_research.utils import touch_file
from siptools_research.workflow.utils import iter_transfers
from siptools_research.shell import Validator
from siptools_research.workflow.task import WorkflowTask
from siptools_research.create_sip.virus import ScanVirus


class ValidateDigitalSignature(WorkflowTask):

    """Validate digital signature in all SIPs under `<workspace>/sip-in-progress`
       directory.


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

        return TaskFileTarget(self.workspace, 'validation-digital-signature')

    def run(self):
        """Do the task.

        :returns: None

        """
        (outcome, detail_note, signature_filename) = check_signature(
            self.sip_creation_path)

        signature_log = TaskLogTarget(self.workspace, 'check_signature')
        with signature_log.open('w') as outfile:
            outfile.write(detail_note)

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.check_signature')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        if outcome == 'success':
            success_log = TaskLogTarget(self.workspace, 'check_signature-success')
            with success_log.open('w') as outfile:
                outfile.write(detail_note)
            task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'success',
                    'messages': ("Transfer signature is valid.")
            }
            mongo_task.write(task_result)
            # task output
            touch_file(TaskFileTarget(self.workspace,
                'validation-digital-signature'))
        else:
            # failed signature
            task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'failure',
                    'messages': detail_note
            }
            mongo_status.write('rejected')
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
            mongo_task.write(task_result)

            failed_log = FailureLog(self.workspace).output()
            with failed_log.open('w') as outfile:
                outfile.write(detail_note)
            yield MoveSipToUser(
                    workspace=self.workspace,
                    home_path=self.home_path)


def check_signature(sip_path):
    """Determine signature filename and validate it.

    :sip_path: Path to SIP
    :returns: (outcome, detail_note, signature_filename)
    """

    signature_path = os.path.join(sip_path, 'varmiste.sig')
    if not os.path.isfile(signature_path):
        signature_path = os.path.join(sip_path, 'signature.sig')

    command = ['verify-signed-file', '-s', signature_path, 'sahke2.xml']
    validator = Validator(command)
    return (validator.outcome, str(validator),
            os.path.basename(signature_path))
