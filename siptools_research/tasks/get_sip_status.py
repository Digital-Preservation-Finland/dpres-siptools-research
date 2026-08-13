"""External task that waits for SIP validation in DPS."""
from datetime import datetime
from pathlib import Path

from luigi import LocalTarget

from siptools_research.dps import get_dps, get_dps_v2
from siptools_research.metax import get_metax_client
from siptools_research.tasks.send_sip import SendSIPToDP
from siptools_research.workflowtask import WorkflowExternalTask

# Transfer statuses in DPS that indicate that the SIP has been validated.
PROCESSED_STATUSES = ("accepted", "rejected")


class GetSIPStatus(WorkflowExternalTask):
    """Task that completes when SIP has been validated.

    The SIP is validated when the transfer created by SendSIPToDP has
    reached "accepted" or "rejected" status in the digital
    preservation system. The status is written to the output file.

    Task requires that SIP is sent to digital preservation service.
    """

    success_message = "SIP validation status resolved."
    failure_message = "SIP validation status could not be resolved."

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: SendSIPToDP task
        """
        return SendSIPToDP(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns:
        `<workspace>/preservation/task-get-validation-reports.finished`,
        which contains the status ("accepted" or "rejected") of the
        transfer.
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.workspace.preservation
                / "task-get-validation-reports.finished")
        )

    def complete(self):

        if self.output().exists():
            # Task is already completed as the status has already
            # been resolved
            return True

        if not self.input().exists():
            # SIP has not even been sent to DPS so checking the
            # status is a waste of time
            return False

        content = Path(self.input().path).read_text().strip()

        dataset_metadata \
            = get_metax_client(self.config).get_dataset(self.dataset_id)
        contract = dataset_metadata["preservation"]["contract"]

        if content.startswith("Dataset "):
            # TODO: Remove once all datasets sent to DPS using the old
            # SFTP/v2 based SendSIPToDP have been processed.
            status = self._get_legacy_status(contract, content)
        else:
            status = self._get_status(contract, content)

        if status not in PROCESSED_STATUSES:
            # SIP has not been validated yet
            return False

        with self.output().open('w') as target:
            target.write(status)

        return True

    def _get_status(self, contract, transfer_id):
        """Get transfer status from DPS using the REST API v3.

        :param contract: Contract identifier
        :param transfer_id: Transfer identifier
        :returns: Transfer status
        """
        dps = get_dps(contract, self.config)
        return dps.get_transfer(transfer_id).status

    def _get_legacy_status(self, contract, content):
        """Get transfer status from DPS using the REST API v2.

        Used only for SIPs sent to DPS before SendSIPToDP was switched
        to the REST API v3, whose SendSIPToDP output contains a
        "Dataset id=<id>,<timestamp>" string instead of a transfer ID.

        :TODO: Remove once all datasets sent to DPS using the old
            SendSIPToDP have been processed.

        :param contract: Contract identifier
        :param content: Content of the SendSIPToDP output file
        :returns: Transfer status, or ``None`` if no ingest report
            entry is available yet
        """
        sip_to_dp_date = datetime.fromisoformat(content.split(',')[-1])

        dps = get_dps_v2(contract, self.config)
        entries = [
            entry for entry in dps.get_ingest_report_entries(
                self.dataset.sip_identifier
            )
            if sip_to_dp_date <= entry['date']
        ]

        if not entries:
            return None

        return entries[0]["status"]
