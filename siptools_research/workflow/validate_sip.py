"""Luigi external task that waits for SIP validation in digital preservation
service.
"""
from datetime import datetime, timedelta

import dateutil.parser

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowExternalTask
from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.utils.database import Database


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

        :returns: remote target that may exist in six possible locations on
                  digital preservation server:
                  ~/accepted/<send_datepath>/<document_id>.tar/
                  ~/rejected/<send_datepath>/<document_id>.tar/
                  ~/accepted/<send_datepath+1>/<document_id>.tar/
                  ~/rejected/<send_datepath+1>/<document_id>.tar/
                  ~/accepted/<send_datepath+2>/<document_id>.tar/
                  ~/rejected/<send_datepath+2>/<document_id>.tar/
        :rtype: RemoteAnyTarget
        """
        conf = Configuration(self.config)
        database = Database(self.config)

        send_timestamp = database.get_event_timestamp(
            self.document_id, "SendSIPToDP"
        )
        date = dateutil.parser.parse(send_timestamp)
        lim_datetime = datetime.today()

        path = []
        while date < lim_datetime:
            date_str = date.strftime("%Y-%m-%d")
            path.append('accepted/%s/%s.tar' % (date_str, self.document_id))
            path.append('rejected/%s/%s.tar' % (date_str, self.document_id))
            date += timedelta(days=1)

        return RemoteAnyTarget(
            path,
            conf.get('dp_host'),
            conf.get('dp_user'),
            conf.get('dp_ssh_key')
        )
