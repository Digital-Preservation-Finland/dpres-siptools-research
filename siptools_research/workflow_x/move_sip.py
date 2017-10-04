"""Move SIP that did not pass validation to the user home directory.

Writes log file to <workspace>/move-sip-to-user.log

"""

import os
import shutil
import grp
import pwd

from luigi import Parameter

from siptools_research.utils.sudo import set_permissions
from siptools_research.luigi.target import TaskLogTarget, TaskFileTarget, \
        MongoDBTarget
from siptools_research.utils.utils import date_str, touch_file

from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask
from siptools_research.luigi.utils import iter_transfers, iter_sips
from siptools_research.utils.scripts.create_process_report\
    import print_collections


class MoveSipToUserLog(WorkflowExternalTask):
    """Log target for MoveSipToUser"""

    def output(self):
        """Return output target for the task.

        :returns: TaskReportTarget object

        """
        return TaskLogTarget(self.workspace, 'move-sip-to-user')


class MoveSipToUser(WorkflowTask):

    """Move invalid SIP and ingest report to `<user home path>/rejected`.

    If SIP / validation report does not contain validation errors raises
    RequireOutcomeError.

    SIP and report is moved to user directory according to transfer report.

    """

    home_path = Parameter()

    def requires(self):
        """Return dictionary of required tasks.

        :returns: Dictionary of required tasks

        """
        # Voisi kayttaa myos kannassa olevia tietoja
        # requires-vaatimuksina tekemalla external taskin MongoDBTargetista
        # document_id = os.path.basename(self.workspace)
        # mongo_target = MongoDBTarget(document_id, "wf_tasks.virus-check")

        return {
            "failure": FailureLog(workspace=self.workspace)
        }

    def output(self):
        """Return output target for the task.

        :returns: TaskReportTarget object

        """
        return MoveSipToUserLog(self.workspace).output()

    def run(self):
        """Do the task.

        :returns: None

        """

        document_id = os.path.basename(self.workspace)
        username = MongoDBTarget(document_id, "username").read()
        sip_name = MongoDBTarget(document_id, "transfer_name").read()
        logs_path = os.path.join(self.workspace, 'logs')

        for sip_path in iter_sips(self.workspace):
            move_sip(self.workspace, username, self.home_path, sip_name,
                     sip_path, document_id, logs_path)

        with self.output().open('w') as outfile:
            outfile.write(
                "Invalid SIP was moved to %s" % self.home_path)


def move_sip(workspace, username, home_path, sip_name, sip_path, document_id,
             logs_path):
    """Move extracted SIP from `workspace` to users `rejected_path`.

    :workspace: Path to SIP workspace
    :rejected_path: Users rejected directory
    :returns: None

    """

    uid = pwd.getpwnam(username).pw_uid
    gid = grp.getgrnam("siptools_research").gr_gid
    rejected_path = os.path.join(home_path, username, 'rejected', date_str())

    transfer_path = iter_transfers(workspace).next()
    rejected_transfer_path = os.path.join(
        rejected_path, os.path.basename(transfer_path))

    if not os.path.isdir(rejected_transfer_path):
        os.makedirs(rejected_transfer_path)

    rejected_sip_path = os.path.join(rejected_transfer_path, sip_name)
    rejected_logs_path = os.path.join(rejected_transfer_path, 'logs')
    if os.path.exists(rejected_sip_path):
        shutil.rmtree(rejected_sip_path)
    os.rename(sip_path, rejected_sip_path)
    os.rename(logs_path, rejected_logs_path)

    # writes process report to rejected folder
    report_file = 'SIP-report-%s.xml' % document_id
    query = {"_id": document_id}
    print_collections(rejected_transfer_path, report_file, query)

    # also copies report of rejected SIP if one exists
    rejected_report = os.path.join(workspace, 'reports',
                                   (os.path.basename(workspace) +
                                    '-rejected.xml'))
    if os.path.isfile(rejected_report):
        new_report = os.path.join(rejected_transfer_path,
                                  os.path.basename(rejected_report))
        shutil.copy(rejected_report, new_report)

    set_permissions(uid, gid, 0o660, rejected_transfer_path)

    # stops luigi by writing all required task output files

    touch_file(TaskFileTarget(workspace, 'virus'))
    touch_file(TaskFileTarget(workspace, 'validation-digital-signature'))
    touch_file(TaskFileTarget(workspace, 'create-ead3'))
    touch_file(TaskFileTarget(workspace, 'create-descriptive-metadata'))
    touch_file(TaskFileTarget(workspace, 'create-provenance-information'))
    touch_file(TaskFileTarget(workspace, 'create-technical-metadata'))
    touch_file(TaskFileTarget(workspace, 'create-structmap'))
    touch_file(TaskFileTarget(workspace, 'create-mets'))
    touch_file(TaskFileTarget(workspace, 'compress-sip'))
    touch_file(TaskFileTarget(workspace, 'send-sip-to-dp'))
    touch_file(TaskFileTarget(workspace, 'ready-for-cleanup'))


class FailureLog(WorkflowExternalTask):
    """Target for detecting failing task"""

    def output(self):
        """Target is failure"""
        return TaskLogTarget(self.workspace, 'failure')
