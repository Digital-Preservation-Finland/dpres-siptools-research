"""Luigi task that sends compressed SIP to DP service."""

import os
import paramiko
import luigi
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.compress import CompressSIP
from siptools_research.utils.contextmanager import redirect_stdout


class SendSIPToDP(WorkflowTask):
    """Copy compressed SIP to ~/transfer directory in digital preservation
    server using SFTP."""
    success_message = "SIP was sent to digital preservation"
    failure_message = "Sending SIP to digital preservation failed"

    def requires(self):
        """Requires tar-archived SIP file.

        :returns: CompressSIP task
        """
        return CompressSIP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Outputs log file: ``logs/task-send-sip-to-dp``.

        :returns: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.logs_path,
                                              'task-send-sip-to-dp.log'))


    def run(self):
        """Sends SIP file to DP service using sftp.

        :returns: None
        """

        with self.output().open('w') as log:
            with redirect_stdout(log):
                # Read host/user/ssh_key_path from onfiguration file
                conf = Configuration(self.config)

                # Init SFTP connection
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(conf.get('dp_host'),
                                username=conf.get('dp_user'),
                                key_filename=conf.get('dp_ssh_key'))

                    with ssh.open_sftp() as sftp:
                        # Copy tar to remote host
                        tar_file = os.path.basename(self.workspace) + '.tar'
                        sftp.put(os.path.join(self.workspace, tar_file),
                                 'transfer/' + tar_file,
                                 confirm=False)
