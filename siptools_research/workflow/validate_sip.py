"""Luigi external task that waits for SIP validation in digital preservation
service."""

import time
from siptools_research.luigi.task import WorkflowExternalTask
from siptools_research.luigi.target import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP

DP_HOST = '86.50.168.218'
DP_USER = 'tpas'
DP_SSH_KEY = '/home/vagrant/.ssh/id_rsa_tpas_pouta'

class ValidateSIP(WorkflowExternalTask):
    """External task that finishes when SIP is found in ~/rejected/ or
    ~/accepted/ directories at digital preservation server.
    """

    def requires(self):
        """Returns list of required tasks. This task requires task:
        CreateWorkspace.

        :returns: CreateWorkspace task
        """
        return SendSIPToDP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Returns output target. This task reports to mongodb when task is
        succesfully completed.

        :returns: RemoteAnyTarget
        """
        date = time.strftime("%Y-%m-%d")
        path = ['accepted/%s/%s' % (date, self.document_id),
                'rejected/%s/%s' % (date, self.document_id)]
        return RemoteAnyTarget(path, DP_HOST,
                               username=DP_USER,
                               key_file=DP_SSH_KEY)
