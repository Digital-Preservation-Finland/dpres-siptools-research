"""Luigi task that sends SIP to DP service."""

import luigi
from siptools_research.dps import get_dps
from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.tasks.compress import CompressSIP


class SendSIPToDP(WorkflowTask):
    """Upload SIP to digital preservation system.

    The SIP is uploaded to the digital preservation system using the
    DPS REST API v3. The transfer ID returned by the API is stored in
    the output file, so that later tasks can poll the status of the
    transfer and retrieve its validation report.

    Task requires that tar archive format SIP is created.
    """

    success_message = "SIP was sent to digital preservation"
    failure_message = "Sending SIP to digital preservation failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CompressSIP task
        """
        return CompressSIP(dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Return output target of this Task.

        :returns: `<workspace>/preservation/task-send-sip-to-dp.finished`,
        which contains the transfer ID of the uploaded SIP.
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            str(self.workspace.preservation / 'task-send-sip-to-dp.finished')
        )

    def run(self):
        """Upload SIP to DP service using the DPS REST API.

        :returns: ``None``
        """
        dataset_metadata \
            = get_metax_client(self.config).get_dataset(self.dataset_id)
        dps = get_dps(
            dataset_metadata["preservation"]["contract"],
            self.config
        )

        uploader = dps.create_uploader(file_path=self.input().path)
        uploader.upload()
        transfer_id = uploader.url.split("/")[-1]

        with self.output().open('w') as output:
            output.write(transfer_id)
