"""Read DP service validation report and set status of SIP package.
Possible outcomes: SIP is accepted or rejected.
"""

import os
import datetime
import traceback

from luigi import Parameter

from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget, \
        TaskLogTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow_c.poll_reports import PollValidationReports


class SetSIPStatus(WorkflowTask):

    """Read DP service validation report and set SIP status.

    """
    workspace = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires validation report.

        """
        return {
            "poll validation reports":
            PollValidationReports(workspace=self.workspace,
                                  home_path=self.home_path)}

    def output(self):
        """Return output target for the task.

        :returns: TaskFileTarget object

        """
        return TaskFileTarget(self.workspace, 'ready-for-cleanup')

    def run(self):
        """Reads validation report and sets status of SIP as either
        accepted or rejected based on the report.
        If SIP is rejected in DP service ingest or if report could not
        be read writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None
        """
        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.set-sip-status')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        report_loc = os.path.join(self.workspace, 'reports')
        accepted_report = os.path.join(
            report_loc, ('%s-accepted.xml' % document_id))
        rejected_report = os.path.join(
            report_loc, ('%s-rejected.xml' % document_id))
        status_log = TaskLogTarget(self.workspace, 'set-sip-status')

        try:
            if os.path.isfile(accepted_report):
                messages = ("Validation report read. Its outcome is "
                            "success. SIP accepted in DP service ingest.")
                task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'success',
                    'messages': messages
                }
                mongo_task.write(task_result)
                mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
                mongo_status.write('accepted')

                with status_log.open('w') as outfile:
                    outfile.write(messages)

                # task output
                touch_file(TaskFileTarget(self.workspace, 'ready-for-cleanup'))

            elif os.path.isfile(rejected_report):
                messages = ("Validation report read. Its outcome is "
                            "failure. SIP rejected in DP service ingest.")
                task_result = {
                    'timestamp': datetime.datetime.utcnow().isoformat(),
                    'result': 'failure',
                    'messages': messages
                }
                mongo_task.write(task_result)
                mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
                mongo_status.write('rejected')

                failed_log = FailureLog(self.workspace).output()
                with failed_log.open('w') as outfile:
                    outfile.write(messages)

                yield MoveSipToUser(
                    workspace=self.workspace,
                    home_path=self.home_path)

                # task output
                touch_file(TaskFileTarget(self.workspace, 'ready-for-cleanup'))

        except:
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'failure',
                'messages': traceback.format_exc()
            }
            mongo_task.write(task_result)

            failed_log = FailureLog(self.workspace).output()
            with failed_log.open('w') as outfile:
                outfile.write("Reading DP validation report failed.")
