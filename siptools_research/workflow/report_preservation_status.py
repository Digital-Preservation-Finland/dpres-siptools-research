"""A workflow task that reads the ingest report locations from preservation
service and updates preservation status to Metax."""

import os
from luigi import LocalTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP
from siptools_research.utils import metax
from siptools_research.utils import contextmanager
from siptools_research.luigi.task import InvalidDatasetError
from siptools_research.config import Configuration
from siptools_research.utils import mail

class ReportPreservationStatus(WorkflowTask):
    """A luigi task that copies and reads the ingest report from preservation
    service. The preservation status is updated to Metax."""

    # If this task fails, it almost always means that SIP was rejected, so this
    # task should not be retried if it fails.
    retry_count = 1
    success_message = "Dataset was accepted to preservation"
    failure_message = "Dataset was not accepted to preservation"

    def requires(self):
        """Requires SIP to be sent to digital preservation service and the
        validation to be finished

        :returns: list of required tasks"""
        return [ValidateSIP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config),
                SendSIPToDP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config)]

    def output(self):
        """Outputs log to ``logs/report-preservation-status.log``

        :returns: None
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

        :returns: None
        """

        with self.output().open('w') as log:
            with contextmanager.redirect_stdout(log):
                # List of all matching paths ValidateSIP found
                ingest_report_paths = self.input()[0].existing_paths()

                # Only one ingest report should be found
                assert len(ingest_report_paths) == 1

                # 'accepted' or 'rejected'?
                metax_client = metax.Metax(self.config)
                directory = ingest_report_paths[0].split('/')[0]
                if directory == 'accepted':
                    # Set Metax preservation state of this dataset to 6 ("in
                    # longterm preservation")
                    metax_client.set_preservation_state(
                        self.dataset_id, '6', 'Accepted to preservation'
                    )
                elif directory == 'rejected':
                    print '\n*********************************************'
                    dataset_metadata = metax_client.get_data('datasets',\
                                                             self.dataset_id)
                    print '\n*********************************************'
                    print dataset_metadata
                    email = dataset_metadata['research_dataset']\
                                            ['rights_holder']['email']
                    conf = Configuration(self.config)
                    email_subject = conf.get('sip_rejected_mail_subject')
                    email_message = conf.get('sip_rejected_mail_msg').format(conf.get('tpas_admin_email'))
                    email_sender = conf.get('tpas_mail_sender')
                    mail.send(email_sender, email,\
                              email_subject, email_message)
                    # Raise exception that informs event handler to set Metax
                    # preservation state of this dataset to 7 ("Rejected
                    # long-term preservation")
                    raise InvalidDatasetError("SIP was rejected")
                else:
                    raise ValueError(' report was found in incorrect'\
                                     'path: %s' % ingest_report_paths[0])
