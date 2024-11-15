"""External task that waits for SIP validation in DPS."""

import os
from pathlib import Path
from datetime import datetime
from luigi import LocalTarget

from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.workflowtask import WorkflowTask
from siptools_research.dps import get_dps
from siptools_research.metax import get_metax_client


class ValidateSIP(WorkflowTask):
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
        If SIP was accepted, the reports are in path ingest-reports/accepted/.
        If the SIP was rejected the reports are in path ingest-reports/rejected/.

        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.validation_workspace / "ingest-reports")
        )

    def run(self):
        input_file = Path(self.input().path)

        sip_to_dp_str = input_file.read_text().split(',')[-1].split('=')[-1]
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
            with self.output().temporary_path() as target_path:
                os.mkdir(target_path)
                for entry in entries:
                    if sip_to_dp_date <= entry['date']:
                        self._write_file(entry, target_path, 'xml', dps, objid)
                        self._write_file(
                            entry, target_path, 'html', dps, objid
                        )
        else:
            raise ValueError("Ingest report not available yet.")

    def _write_file(self, entry, target_path, file_type, dps, objid):
        status = entry['status']
        transfer_id = entry['transfer_id']
        path = Path(
            f"{target_path}/{status}/{transfer_id}.{file_type}"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(dps.get_ingest_report(
                            objid, transfer_id, file_type
                            ).decode()
                        )
