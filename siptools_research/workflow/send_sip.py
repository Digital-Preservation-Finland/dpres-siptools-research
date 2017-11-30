"""Sends compressed SIP to DP service."""

import os
import subprocess

from luigi import Parameter

from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.sign import SignSIP
from siptools_research.utils import utils
import siptools_research.utils.database
from siptools_research.utils.contextmanager import redirect_stdout

IDENTITY = '~/.ssh/id_rsa_tpas_pouta'
class SendSIPToDP(WorkflowTask):
    """Send SIP to DP.
    """
    retry_count = 10
    sip_path = Parameter()
    dataset_id = Parameter()

    def requires(self):
        """Requires compressed SIP archive file.
        """
        return {"Sign SIP":
                SignSIP(workspace=self.workspace,
                        dataset_id=self.dataset_id,
                        config=self.config)}

    def output(self):
        """Returns task output. Task is ready when succesful event has been
         added to worklow database.

         :returns: MongoTaskResultTarget
        """
        return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name,
                                     config_file=self.config)


    def run(self):
        """Sends SIP file to DP service using sftp.
        If unsuccessful for more times than the amount specified by
        count task writes an error message into mongoDB, updates
        the status of the document to pending and completes itself.

        :returns: None
        """

        sip_name = os.path.join(self.sip_path,
                                (os.path.basename(self.workspace) + '.tar'))

        sent_log = os.path.join(self.workspace,
                                "logs",
                                'task-send-sip-to-dp.log')
        utils.makedirs_exist_ok(os.path.join(self.workspace, "logs"))
        task_result = None
        open(sent_log, 'a')
        try:
            with open(sent_log, 'w+') as log:
                with redirect_stdout(log):
                    (outcome, err) = send_to_dp(sip_name)
                    if outcome == 'success':
                        task_result = 'success'
                        task_messages = "Send to DP successfully "
                    else:
                        task_result = 'failure'
                        task_messages = err

        finally:
            if not task_result:
                task_result = 'failure'
                task_messages = "Sending SIP to dp "\
                                "failed due to unknown error."

            database = siptools_research.utils.database.Database(self.config)
            database.add_event(self.document_id,
                               self.task_name,
                               task_result,
                               task_messages)


def send_to_dp(sip):
    """Sends SIP to DP service using sftp.
    """
    print "send-to-dp"
    identity = IDENTITY
    host = '86.50.168.218'
    username = 'tpas'

    connection = '%s@%s:transfer' % (username, host)
    identityfile = '-oIdentityFile=%s' % identity
    outcome = 'failure'

    command = 'put %s' % sip
    cmd = 'sftp %s %s' % (identityfile, connection)

    ssh = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=True)
    out, err = ssh.communicate(command)
    returncode = ssh.returncode

    if returncode == 0:
        print 'Transfer completed successfully.'
        outcome = 'success'
        print "return 0"

    return outcome, err
