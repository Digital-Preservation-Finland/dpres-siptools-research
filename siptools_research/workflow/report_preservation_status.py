"""Task that reports preservation status after SIP ingestion."""

from luigi import LocalTarget
from metax_access import DS_STATE_IN_DIGITAL_PRESERVATION
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.exceptions import InvalidSIPError


class ReportPreservationStatus(WorkflowTask):
    """Task that reports preservation status after SIP ingestion.

    A workflowtask that updates the preservation status of dataset in
    Metax based on the directory where ingest report was found in
    digital preservation system.

    A false target `report-preservation-status.finished` is created into
    preservation workspace directory to notify luigi that this task has
    finished.

    Task requires SIP to be sent to digital preservation service and the
    validation to be finished.
    """

    success_message = "Dataset was accepted to preservation"
    failure_message = "Dataset was not accepted to preservation"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return [ValidateSIP(dataset_id=self.dataset_id,
                            config=self.config),
                SendSIPToDP(dataset_id=self.dataset_id,
                            config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/preservation/`
                  `report-preservation-status.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.preservation_workspace
                / 'report-preservation-status.finished')
        )

    def run(self):
        """Report preservation status to Metax.

        Checks the path of ingest report file in digital preservation
        service. If the ingest report is in ~/accepted/.../ directory,
        the dataset has passed validation.If the report is found in
        ~/rejected/.../ directory, or somewhere else, an exception is
        risen. The event handlers will deal with the exceptions.

        :returns: ``None``
        """
        # List of all matching paths ValidateSIP found
        ingest_report_paths = self.input()[0].existing_paths()

        # Only one ingest report should be found
        if len(ingest_report_paths) != 1:
            raise ValueError(
                f"Expected 1 ingest report, found {len(ingest_report_paths)}"
            )

        # 'accepted' or 'rejected'?
        directory = ingest_report_paths[0].split('/')[0]

        if directory == 'accepted':
            # Set the preservation state of this dataset
            self.dataset.set_preservation_state(
                DS_STATE_IN_DIGITAL_PRESERVATION,
                'Accepted to preservation'
            )
            with self.output().open('w') as output:
                output.write('Dataset id=' + self.dataset_id)
        elif directory == 'rejected':
            # Raise exception that informs event handler that dataset
            # did not pass validation
            raise InvalidSIPError("SIP was rejected")
        else:
            raise ValueError(
                f'Report was found in incorrect path: {ingest_report_paths[0]}'
            )
