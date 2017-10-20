"""Sends compressed SIP to DP service."""

import os
import sys
import traceback
import datetime
import subprocess

from luigi import Parameter

from siptools_research.workflow_x.move_sip import FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask

from siptools_research.workflow.compress import CompressSIP


class SendSIPToDP(WorkflowTask):
    """Send SIP to DP.
    """
    home_path = Parameter()

    def requires(self):
        """Requires compressed SIP archive file.
        """
        return {
            "Compress SIP":
            CompressSIP(workspace=self.workspace,
                        sip_creation_path=self.sip_creation_path,
                        sip_output_path=self.sip_output_path,
                        home_path=self.home_path)}

    def output(self):
        """Returns task output. Task is ready when succesful event has been
         added to worklow database.

         :returns: MongoTaskResultTarget
        """  
        return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name)
       

    def run(self):
        """Sends SIP file to DP service using sftp.
        If unsuccessful for more times than the amount specified by
        count task writes an error message into mongoDB, updates
        the status of the document to pending and completes itself.

        :returns: None
        """

        sip_name = os.path.join(self.sip_output_path,
                                (os.path.basename(self.workspace) + '.tar'))

        # Check how many times task has been run
        sent_log = os.path.join(self.workspace,
                                  "logs",
                                  'task-send-sip-to-dp.log')

        try:
             with open(sent_log, 'w') as log:
                 with redirect_stdout(log):

                   (outcome, err) = send_to_dp(sip_name, sent_log, count)
                   if outcome == 'success':
                      task_result = 'success'
                      task_messages = "Send to DP successfully "
                   else: 
                      task_result = 'failure'
                      task_messages =  err

        finally:
                if not task_result:
                    task_result = 'failure'
                    task_messages = "Sending SIP to dp "\
                                "failed due to unknown error."

                database.add_event(self.document_id,
                               self.task_name,
                               task_result,
                               task_messages)

class SendSIPComplete(WorkflowExternalTask):
    """Task that completes after SIP has been sent to DP.
    """

    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'send-sip-to-dp')


def send_to_dp(sip, sent_log, count):
    """Sends SIP to DP service using sftp.
    """
    count += 1
    identity = '~/.ssh/id_rsa_tpas_pouta'
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
    print out

    if returncode == 0:
        print 'Transfer completed successfully.'
        outcome = 'success' 


    return outcome, err
