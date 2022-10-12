"""Task that reports preservation status after SIP ingestion."""

import os
from luigi import LocalTarget
from metax_access import Metax, DS_STATE_IN_DIGITAL_PRESERVATION
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.exceptions import InvalidSIPError
from siptools_research.config import Configuration


class ReportPreservationStatus(WorkflowTask):
    """Task that reports preservation status after SIP ingestion.

    A workflowtask that updates the preservation status of dataset in
    Metax based on the directory where ingest report was found in
    digital preservation system.

    A false target `report-preservation-status.finished` is created into
    workspace directory to notify luigi (and dependent tasks) that this
    task has finished.

    Task requires SIP to be sent to digital preservation service and the
    validation to be finished.
    """

    success_message = "Dataset was accepted to preservation"
    failure_message = "Dataset was not accepted to preservation"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return [ValidateSIP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config),
                SendSIPToDP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/report-preservation-status.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace, 'report-preservation-status.finished'
            )
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
        config = Configuration(self.config)

        # Only one ingest report should be found
        if len(ingest_report_paths) != 1:
            raise ValueError(
                "Expected 1 ingest report, found {}".format(
                    len(ingest_report_paths)
                )
            )

        # 'accepted' or 'rejected'?
        directory = ingest_report_paths[0].split('/')[0]

        if directory == 'accepted':
            # Init metax
            metax_client = Metax(
                config.get('metax_url'),
                config.get('metax_user'),
                config.get('metax_password'),
                verify=config.getboolean('metax_ssl_verification')
            )
            # Set Metax preservation state of this dataset to 6 ("in
            # longterm preservation")
            metax_client.set_preservation_state(
                self.dataset_id,
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
            raise ValueError('Report was found in incorrect '
                             'path: %s' % ingest_report_paths[0])
