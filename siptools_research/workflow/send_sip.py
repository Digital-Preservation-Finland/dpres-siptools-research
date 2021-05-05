"""Luigi task that sends SIP to DP service."""

import os
import paramiko
import luigi
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.compress import CompressSIP


class SendSIPToDP(WorkflowTask):
    """Copy SIP to ~/transfer directory in digital preservation system.

    As the SIP file is moved from the transfer directory by preservation
    system the existence of the SIP file cannot be verified reliably
    there. A false target file in workspace:
    `task-send-sip-to-dp.finished` is used as an output to notify
    luigi(and dependent tasks) that this task has finished and SIP file
    has been transferred to preservation system.

    Task requires that tar archive format SIP is created.
    """

    success_message = "SIP was sent to digital preservation"
    failure_message = "Sending SIP to digital preservation failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CompressSIP task
        """
        return CompressSIP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Return output target of this Task.

        :returns: `<workspace>/task-send-sip-to-dp.finished`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, 'task-send-sip-to-dp.finished'),
        )

    def run(self):
        """Send SIP file to DP service using sftp.

        :returns: ``None``
        """
        # Read host/user/ssh_key_path from onfiguration file
        conf = Configuration(self.config)

        # Init SFTP connection
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=conf.get('dp_host'),
                        port=int(conf.get('dp_port')),
                        username=conf.get('dp_user'),
                        key_filename=conf.get('dp_ssh_key'))

            with ssh.open_sftp() as sftp:
                # Copy tar to remote host. Validation workflow starts
                # when ".incomplete" suffix is removed from target file
                # path.
                tar_file = os.path.basename(self.workspace) + '.tar'
                sftp.put(os.path.join(self.workspace, tar_file),
                         os.path.join('transfer', tar_file + '.incomplete'),
                         confirm=False)
                sftp.rename(
                    os.path.join(
                        'transfer', tar_file + '.incomplete'
                    ),
                    os.path.join('transfer', tar_file)
                )

            with self.output().open('w') as log:
                log.write('Dataset id=' + self.dataset_id)
