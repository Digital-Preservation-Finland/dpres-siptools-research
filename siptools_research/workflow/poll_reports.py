"""External task that waits for SIP validation in DPS."""
from pathlib import Path
from datetime import datetime
from luigi import LocalTarget

from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.workflowtask import WorkflowExternalTask
from siptools_research.dps import get_dps
from siptools_research.metax import get_metax_client


class GetValidationReports(WorkflowExternalTask):
    """Task that completes when SIP has been validated.

    The SIP is validated when ingest report is available in ~/rejected/
    or ~/accepted/ directories in digital preservation system.
    Ingest reports are fetched using the DPS's REST API, where they
    are loaded to the workspace.

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

        :returns: `<workspace>/validation/ingest-resports/`, where
        the ingest reports are loaded in xml and html format.
        Dependign of the status of the SIP,
        the reports are in path `ingest-reports/accepted/` or
        `ingest-reports/rejected/`.

        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.validation_workspace / "ingest-reports")
        )

    def complete(self):

        if (self.dataset.validation_workspace / "ingest-reports").exists():
            # Task is already completed as the ingest report folder exists
            return True


        if not self.input().exists():
            # SIP has not even been sent to DPS so checking
            # for ingest report is waste of time
            return False

        input_file = Path(self.input().path)

        sip_to_dp_str = input_file.read_text().split(',')[-1]
        sip_to_dp_date = datetime.fromisoformat(sip_to_dp_str)

        dataset_metadata \
            = get_metax_client(self.config).get_dataset(self.dataset_id)
        objid = dataset_metadata.get("preservation", {}).get("id")

        dps = get_dps(
            dataset_metadata.get("preservation", {}).get("contract"),
            self.config
        )
        entries = dps.get_ingest_report_entries(objid)
     

        if entries:
            with self.output().temporary_path() as target:
                target_path = Path(target)
                target_path.mkdir()
                for entry in entries:
                    if sip_to_dp_date <= entry['date']:
                        self._write_file(
                            entry, target_path, 'xml', dps, objid
                        )
                        self._write_file(
                            entry, target_path, 'html', dps, objid
                        )
        else:
            # Ingest report not available yet
            return False

        return True

    def _write_file(self, entry, target_path, file_type, dps, objid):
        status = entry['status']
        transfer_id = entry['transfer_id']
        path = target_path / status / f"{transfer_id}.{file_type}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dps.get_ingest_report(
                            objid, transfer_id, file_type
                            ).decode()
                        )
