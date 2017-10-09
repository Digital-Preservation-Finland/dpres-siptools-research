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
    workspace = Parameter()
    sip_creation_path = Parameter()
    sip_output_path = Parameter()
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
        """Return output target for the task.

        :returns: TaskFileTarget object
        """
        return TaskFileTarget(self.workspace, 'send-sip-to-dp')

    def run(self):
        """Sends SIP file to DP service using sftp.
        If unsuccessful for more times than the amount specified by
        count task writes an error message into mongoDB, updates
        the status of the document to pending and completes itself.

        :returns: None
        """
        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.send-sip-to-dp-service')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        sip_name = os.path.join(self.sip_output_path,
                                (os.path.basename(self.workspace) + '.tar'))

        # Check how many times task has been run
        count = 0
        sent_log = os.path.join(self.workspace, 'logs',
                                'task-send-sip-to-dp.log')
        if os.path.isfile(sent_log):
            log_file = open(sent_log, 'r')
            first_line = log_file.readline()
            count = int(first_line.split(' ', 1)[0])
            log_file.close()

        # Run task if count is < 10
        if count < 10:
            try:
                (outcome, err) = send_to_dp(sip_name, sent_log, count)

                if outcome == 'success':
                    task_result = {
                        'timestamp': datetime.datetime.utcnow().isoformat(),
                        'result': 'success',
                        'messages': ("SIP sent to DP service for "
                                     "ingest.")
                    }
                    mongo_task.write(task_result)
                    mongo_timestamp.write(
                        datetime.datetime.utcnow().isoformat())
                    mongo_status.write('SIP sent to DP service')

                    # task output
                    touch_file(TaskFileTarget(self.workspace,
                                              'send-sip-to-dp'))

                else:
                    task_result = {
                        'timestamp': datetime.datetime.utcnow().isoformat(),
                        'result': 'failure',
                        'messages': err
                    }
                    mongo_task.write(task_result)

                    failed_log = FailureLog(self.workspace).output()
                    with failed_log.open('w') as outfile:
                        outfile.write('Failed to send SIP to DP.')

            except:
                task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'failure',
                    'messages': traceback.format_exc()
                }
                mongo_task.write(task_result)

                failed_log = FailureLog(self.workspace).output()
                with failed_log.open('w') as outfile:
                    outfile.write('Failed to send SIP to DP.')

        # Complete task and set status to pending
        elif count == 10:
            mongo_status.write('pending')
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())

            # task output
            touch_file(TaskFileTarget(self.workspace, 'send-sip-to-dp'))


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
    identity = '~/.ssh/id_rsa_pastesti_Kansallisarkisto'
    host = '86.50.168.61'
    username = 'kansallisarkisto'

    connection = '%s@%s:transfer' % (username, host)
    identityfile = '-oIdentityFile=%s' % identity
    outcome = 'failure'

    command = 'put %s' % sip
    cmd = 'sftp %s %s' % (identityfile, connection)

    # Write stdout to log
    save_stdout = sys.stdout
    log = open(sent_log, 'w')
    sys.stdout = log
    print count

    ssh = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=True)

    out, err = ssh.communicate(command)
    returncode = ssh.returncode
    print out

    if returncode == 0:
        print 'Transfer completed successfully.'
        outcome = 'success'
    else:
        print err

    # Close log
    sys.stdout = save_stdout
    log.close()

    return outcome, err
