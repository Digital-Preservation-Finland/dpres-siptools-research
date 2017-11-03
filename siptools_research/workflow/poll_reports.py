"""Poll DP service for validation report.
"""

import os
import sys
import traceback
import datetime
import subprocess

from luigi import Parameter

from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.send_sip import SendSIPToDP


class PollValidationReports(WorkflowTask):
    """Poll DP service for validation report. Copy validation report to
    <workspace>/reports folder.
    Task completes itselft and sets the status of the SIP to pending
    after trying a number of times, specified by the count variable.
    """
    workspace = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires completed transfer of SIP to DP service.
        """
        return {
            "SIP sent to DP service": SendSIPToDP(
                workspace=self.workspace)}

    def output(self):
        """Return output target for the task.

        :returns: TaskFileTarget object
        """
        return TaskFileTarget(self.workspace, 'report-file')

    def run(self):
        """Do the task.

        :returns: None
        """
        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.poll-dp-validation-reports')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        report_path = os.path.join(self.workspace, 'reports')

        if not os.path.isdir(report_path):
            os.makedirs(report_path)

        # Check how many times task has been run
        count = 0
        report_log = os.path.join(self.workspace, 'logs',
                                  'task-poll-reports.log')
        if os.path.isfile(report_log):
            log_file = open(report_log, 'r')
            first_line = log_file.readline()
            count = int(first_line.split(' ', 1)[0])
            log_file.close()

        # Run task if task hasn't been run too many times
        if count < 10:
            try:
                report_result = get_reports(document_id, report_path,
                                            report_log, count)

                if report_result == 'success':

                    task_result = {
                        'timestamp': datetime.datetime.utcnow().isoformat(),
                        'result': 'success',
                        'messages': ("DP service polled for validation "
                                     "reports. Validation report found and "
                                     "copied to workspace.")
                    }
                    mongo_task.write(task_result)

                    # task output
                    touch_file(TaskFileTarget(self.workspace, 'report-file'))

            except:
                task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'failure',
                    'messages': traceback.format_exc()
                }
                mongo_task.write(task_result)


        # Complete task and sets status to pending
        elif count == 10:
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'failure',
                'messages': 'No reports found.'
            }
            mongo_status.write('pending')
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace, 'report-file'))


def get_reports(document_id, report_path, report_log, count):
    """Polls DP service for validation report.
    Validation report is either in the accepted or rejected folder,
    depending on validation outcome.
    Copies validation report from DP service to workspace.
    Adds '-rejected' or '-accepted' to validation report name.
    """
    # Update count and write stdout to log
    count += 1
    save_stdout = sys.stdout
    log = open(report_log, 'w')
    sys.stdout = log
    print count

    result = ''

    # Poll accepted reports
    accepted = ssh_to_dp(document_id, report_path, status='accepted')

    # Poll rejected reports
    rejected = ssh_to_dp(document_id, report_path, status='rejected')

    # Outcome is successful if report found and transferred
    if accepted == 'success':
        result = 'success'
    elif rejected == 'success':
        result = 'success'

    # Close log
    sys.stdout = save_stdout
    log.close()

    return result


def ssh_to_dp(document_id, report_path, status):
    """Connects to DP service by sftp."""
    # pas-testi hardcoded
    identity = '~/.ssh/id_rsa_pastesti_Kansallisarkisto'
    host = '86.50.168.61'
    username = 'kansallisarkisto'

    report_location = '%s@%s:%s' % (username, host, status)
    identityfile = '-oIdentityFile=%s' % identity

    report = os.path.join(report_path, ('%s-%s.xml' % (document_id, status)))

    # get files
    cmd = 'sftp %s %s' % (identityfile, report_location)
    command = ('get -r */*/%s*.xml %s') % (document_id, report)

    ssh = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, shell=True)
    out, err = ssh.communicate(command)
    returncode = ssh.returncode
    print out
    print err
    print returncode

    outcome = 'failure'

    if os.path.isfile(report):
        outcome = 'success'

    return outcome
