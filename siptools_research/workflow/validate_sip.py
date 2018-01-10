"""Luigi external task that waits for SIP validation in digital preservation
service."""

import time
from siptools_research.config import Configuration
from siptools_research.luigi.task import WorkflowExternalTask
from siptools_research.luigi.target import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP

class ValidateSIP(WorkflowExternalTask):
    """External task that finishes when SIP is found in ~/rejected/ or
    ~/accepted/ directories at digital preservation server.
    """

    def requires(self):
        """Requires SIP to be sent to DP service.

        :returns: SendSIPToDP task
        """
        return SendSIPToDP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Outpus ingest report to
        dp_service:~/accepted/<datepath>/<document_id>/<document_id>-<uuid> or
        dp_service:~/rejected/<datepath>/<document_id>/<document_id>-<uuid>

        :returns: RemoteAnyTarget
        """
        # TODO: if day changes between ingest report creation and init of this
        # target, the target does not exist.
        conf = Configuration(self.config)
        date = time.strftime("%Y-%m-%d")
        path = ['accepted/%s/%s.tar' % (date, self.document_id),
                'rejected/%s/%s.tar' % (date, self.document_id)]
        return RemoteAnyTarget(path, conf.get('dp_host'),
                               username=conf.get('dp_user'),
                               key_file=conf.get('dp_ssh_key'))
