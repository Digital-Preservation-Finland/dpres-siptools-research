"""Luigi task that sends compressed SIP to DP service."""

import os
import subprocess
import luigi
from siptools_research.config import Configuration
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.sign import SignSIP
from siptools_research.utils.contextmanager import redirect_stdout

class SendSIPToDP(WorkflowTask):
    """Send SIP to DP.
    """
    success_message = "SIP was sent to digital preservation"
    failure_message = "Sending SIP to digital preservation failed"

    def requires(self):
        """Requires compressed SIP archive file.
        """
        return {"Sign SIP":
                SignSIP(workspace=self.workspace,
                        dataset_id=self.dataset_id,
                        config=self.config)}

    def output(self):
        """Returns task output.

        :returns: MongoTaskResultTarget
        """
        return luigi.LocalTarget(os.path.join(self.logs_path,
                                              'task-send-sip-to-dp.log'))


    def run(self):
        """Sends SIP file to DP service using sftp.

        :returns: None
        """

        sip_name = os.path.join(self.sip_creation_path,
                                (os.path.basename(self.workspace) + '.tar'))

        with self.output().open('w') as log:
            with redirect_stdout(log):
                (outcome, err) = send_to_dp(sip_name, self.config)
                if not outcome == 'success':
                    raise Exception(err)


def send_to_dp(sip, config_file):
    """Sends SIP to DP service using sftp.
    """
    print "send-to-dp"
    conf = Configuration(config_file)
    identity = conf.get('dp_ssh_key')
    host = conf.get('dp_host')
    username = conf.get('dp_user')

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
