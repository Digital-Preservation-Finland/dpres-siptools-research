"""Luigi external task that waits for SIP validation in digital preservation
service."""

import time
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowExternalTask
from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP


class ValidateSIP(WorkflowExternalTask):
    """External task that finishes when SIP is found in ~/rejected/ or
    ~/accepted/ directories at digital preservation server. Task requires that
    SIP is sent to digital preservation service.
    """

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: SendSIPToDP task
        """
        return SendSIPToDP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """The output that this Task produces.

        :returns: remote target that may exist in two possible locations on
            digital preservation server:
            ~/accepted/<datepath>/<document_id>.tar/ or
            ~/rejected/<datepath>/<document_id>.tar/
        :rtype: RemoteAnyTarget
        """
        # TODO: if day changes between ingest report creation and init of this
        # target, the target does not exist.
        conf = Configuration(self.config)
        date = time.strftime("%Y-%m-%d")
        path = ['accepted/%s/%s.tar' % (date, self.document_id),
                'rejected/%s/%s.tar' % (date, self.document_id)]
        return RemoteAnyTarget(path, conf.get('dp_host'),
                               conf.get('dp_user'),
                               conf.get('dp_ssh_key'))
