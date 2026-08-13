"""Task that reports preservation status after SIP ingestion."""

from pathlib import Path

from luigi import LocalTarget
from metax_access import DS_STATE_IN_DIGITAL_PRESERVATION

from siptools_research.exceptions import InvalidSIPError
from siptools_research.tasks.get_sip_status import GetSIPStatus
from siptools_research.tasks.send_sip import SendSIPToDP
from siptools_research.workflowtask import WorkflowTask


class ReportPreservationStatus(WorkflowTask):
    """Task that reports preservation status after SIP ingestion.

    A workflowtask that updates the preservation status of dataset in
    Metax based on the transfer status resolved by GetSIPStatus.

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
        return [GetSIPStatus(dataset_id=self.dataset_id,
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
            str(self.workspace.preservation
                / 'report-preservation-status.finished')
        )

    def run(self):
        """Report preservation status to Metax.

        Checks the transfer status resolved by GetSIPStatus.
        If the status is "accepted", the dataset has passed
        validation. If the status is "rejected", an exception is
        raised. The event handlers will deal with the exceptions.

        :returns: ``None``
        """
        status = Path(self.input()[0].path).read_text().strip()

        if status == 'accepted':

            # Mark the dataset as preserved
            self.dataset.mark_preserved()
            # Unlock the dataset
            self.dataset.unlock()

            # Set the preservation state of this dataset
            self.dataset.set_preservation_state(
                DS_STATE_IN_DIGITAL_PRESERVATION,
                'In digital preservation'
            )

            with self.output().open('w') as output:
                output.write('Dataset id=' + self.dataset_id)

        elif status == 'rejected':
            # Raise exception that informs event handler that dataset
            # did not pass validation
            raise InvalidSIPError("SIP was rejected")

        else:
            raise ValueError(f'Unexpected transfer status: {status}')
