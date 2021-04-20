"""External task that waits for SIP validation in DPS."""
from datetime import datetime, timedelta

import os
import dateutil.parser

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowExternalTask
from siptools_research.remoteanytarget import RemoteAnyTarget
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.utils.database import Database


class ValidateSIP(WorkflowExternalTask):
    """External task that completes when SIP has been validated.

    The SIP is validated when ingest report is available in ~/rejected/
    or ~/accepted/ directories in digital preservation system.

    Task requires that SIP is sent to digital preservation service.
    """

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: SendSIPToDP task
        """
        return SendSIPToDP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: remote target that may exist on digital preservation
                  server in any path formatted::

                      ~/accepted/<datepath>/<document_id>.tar/
                      ~/rejected/<datepath>/<document_id>.tar/

                  where datepath is any date between the date the SIP
                  was sent to the server and the current date.

        :rtype: RemoteAnyTarget
        """
        conf = Configuration(self.config)
        database = Database(self.config)

        # Get SendSIPToDP completion datetime or use the current UTC
        # time. This is necessary since ValidateSip output is checked
        # first time before any of the dependencies are ran.
        # Dependencies are ran only if ValidateSip task is not
        # completed.
        try:
            send_timestamp = database.get_event_timestamp(
                self.document_id, "SendSIPToDP"
            )
            sip_to_dp_date = dateutil.parser.parse(send_timestamp).date()
        except (ValueError, KeyError):
            sip_to_dp_date = datetime.utcnow().date()

        lim_date = datetime.today().date()
        sftp_root = conf.get('dp_home')

        paths = []
        while sip_to_dp_date <= lim_date:
            paths.append(
                os.path.join(
                    sftp_root,
                    'accepted/%s/%s.tar' % (sip_to_dp_date,
                                            self.document_id)
                )
            )
            paths.append(
                os.path.join(
                    sftp_root,
                    'rejected/%s/%s.tar' % (sip_to_dp_date,
                                            self.document_id)
                )
            )
            sip_to_dp_date += timedelta(days=1)

        return RemoteAnyTarget(
            paths,
            host=conf.get('dp_host'),
            port=int(conf.get('dp_port')),
            username=conf.get('dp_user'),
            keyfile=conf.get('dp_ssh_key')
        )
