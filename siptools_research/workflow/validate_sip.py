"""External task that waits for SIP validation in DPS."""

import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from luigi import LocalTarget
import paramiko

import dateutil.parser
from siptools_research.config import Configuration
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.workflowtask import WorkflowTask


class ValidateSIP(WorkflowTask):
    """External task that completes when SIP has been validated.

    The SIP is validated when ingest report is available in ~/rejected/
    or ~/accepted/ directories in digital preservation system.

    Task requires that SIP is sent to digital preservation service.
    """

    success_message = "Ingest report(s) downloaded succesfully."
    failure_message = "Ingest report(s) download not succesful."

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: SendSIPToDP task
        """
        return SendSIPToDP(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: remote target that may exist on digital preservation
                  server in any path formatted::

                      ~/accepted/<datepath>/<dataset_id>.tar/
                      ~/rejected/<datepath>/<dataset_id>.tar/

                  where datepath is any date between the date the SIP
                  was sent to the server and the current date.

        :rtype: RemoteAnyTarget
        """
        return LocalTarget(
            str(self.dataset.validation_workspace / "ingest-reports")
        )

    def run(self):
        # Get SendSIPToDP completion datetime or use the current UTC
        # time. This is necessary since ValidateSip output is checked
        # first time before any of the dependencies are ran.
        # Dependencies are ran only if ValidateSip task is not
        # completed.
        try:
            send_timestamp = self.dataset.get_task_timestamp("SendSIPToDP")
            sip_to_dp_date = dateutil.parser.parse(send_timestamp).date()
        except (ValueError, KeyError):
            sip_to_dp_date = datetime.now(timezone.utc).date()

        lim_date = datetime.today().date()
        paths = []
        while sip_to_dp_date <= lim_date:
            paths.append(
                os.path.join(
                    f"accepted/{sip_to_dp_date}/{self.dataset_id}.tar"
                )
            )
            paths.append(
                os.path.join(
                    f"rejected/{sip_to_dp_date}/{self.dataset_id}.tar"
                )
            )
            sip_to_dp_date += timedelta(days=1)

        with self.output().temporary_path() as target_path:
            existing_paths = self.existing_paths(paths)
            if len(existing_paths) > 0:
                os.mkdir(target_path)
                for path in existing_paths:
                    full_path = Path(target_path) / path
                    full_path.parent.mkdir(parents=True, exist_ok=True)
                    full_path.touch()

    def existing_paths(self, paths):
        """Returns the paths that exists."""
        conf = Configuration(self.config)
        host = conf.get("dp_host")
        port = int(conf.get("dp_port"))
        username = conf.get("dp_user")
        keyfile = conf.get("dp_ssh_key")
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                host, port=int(port), username=username, key_filename=keyfile
            )
            with ssh.open_sftp() as sftp:
                return [path for path in paths if self._exists(sftp, path)]

    def _exists(self, sftp, path):
        """Returns ``True`` if the path exists in remote host.
        :param path: path to verify at remote host
        """
        try:
            sftp.stat(path)
            return True
        except OSError as ex:
            if "No such file" not in str(ex):
                raise
        return False
