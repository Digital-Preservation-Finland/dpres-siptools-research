"""A luigi task that reads the ingest report locations from preservation
service and updates preservation status to Metax."""

import os
from luigi import LocalTarget
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.utils import contextmanager
from siptools_research.workflowtask import InvalidDatasetError
from siptools_research.config import Configuration
from metax_access import Metax, DS_STATE_IN_DIGITAL_PRESERVATION


class ReportPreservationStatus(WorkflowTask):
    """A workflowtask that updates the preservation status of dataset in Metax
    based on the directory where ingest report was found in digital
    preservation system. Task requires SIP to be sent to digital preservation
    service and the validation to be finished.
    """

    success_message = "Dataset was accepted to preservation"
    failure_message = "Dataset was not accepted to preservation"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of required tasks"""
        return [ValidateSIP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config),
                SendSIPToDP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config)]

    def output(self):
        """The output that this Task produces.

        :returns: local target: `logs/report-preservation-status.log`
        :rtype: LocalTarget
        """
        return LocalTarget(os.path.join(self.logs_path,
                                        'report-preservation-status.log'))

    def run(self):
        """Checks the path of ingest report file in digital preservation
        service. If the ingest report is in ~/accepted/.../ directory, the
        dataset has passed validation. The preservation status is reported to
        Metax. If the report is found in ~/rejected/.../ directory, or
        somewhere else, an exception is risen. The event handlers will deal
        with the exceptions.

        :returns: ``None``
        """

        with self.output().open('w') as log:
            with contextmanager.redirect_stdout(log):
                # List of all matching paths ValidateSIP found
                ingest_report_paths = self.input()[0].existing_paths()

                # Only one ingest report should be found
                assert len(ingest_report_paths) == 1

                # Init metax
                config_object = Configuration(self.config)
                metax_client = Metax(config_object.get('metax_url'),
                                     config_object.get('metax_user'),
                                     config_object.get('metax_password'))

                # 'accepted' or 'rejected'?
                directory = ingest_report_paths[0].split('/')[0]
                if directory == 'accepted':
                    # Set Metax preservation state of this dataset to 6 ("in
                    # longterm preservation")
                    metax_client.set_preservation_state(
                        self.dataset_id,
                        DS_STATE_IN_DIGITAL_PRESERVATION,
                        system_description='Accepted to preservation'
                    )
                elif directory == 'rejected':
                    # Raise exception that informs event handler that dataset
                    # did not pass validation
                    raise InvalidDatasetError("SIP was rejected")
                else:
                    raise ValueError('Report was found in incorrect.'
                                     ' Path: %s' % ingest_report_paths[0])
