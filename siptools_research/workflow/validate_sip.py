"""Luigi external task that waits for SIP validation in digital preservation
service.
"""

from datetime import date, timedelta
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowExternalTask
from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP


class ValidateSIP(WorkflowExternalTask):
    """External task that finishes when SIP is found in ~/rejected/ or
    ~/accepted/ directories at digital preservation server.

    Task requires that SIP is sent to digital preservation service.
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

        :returns: remote target that may exist in four possible locations on
                  digital preservation server:
                  ~/accepted/<datepath-today>/<document_id>.tar/
                  ~/rejected/<datepath-today>/<document_id>.tar/
                  ~/accepted/<datepath-yesterday>/<document_id>.tar/
                  ~/rejected/<datepath-yesterday>/<document_id>.tar/
        :rtype: RemoteAnyTarget
        """
        conf = Configuration(self.config)

        today = date.today().strftime("%Y-%m-%d")
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        path = [
            'accepted/%s/%s.tar' % (today, self.document_id),
            'rejected/%s/%s.tar' % (today, self.document_id),
            'accepted/%s/%s.tar' % (yesterday, self.document_id),
            'rejected/%s/%s.tar' % (yesterday, self.document_id)
        ]

        return RemoteAnyTarget(path, conf.get('dp_host'),
                               conf.get('dp_user'),
                               conf.get('dp_ssh_key'))
