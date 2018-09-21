"""A luigi task that reads the ingest report locations from preservation
service and updates preservation status to Metax."""

import os
from luigi import LocalTarget
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP
from metax_access import Metax, DS_STATE_IN_DIGITAL_PRESERVATION
from siptools_research.utils import contextmanager
from siptools_research.workflowtask import InvalidDatasetError
from siptools_research.config import Configuration
import paramiko


class ReportPreservationStatus(WorkflowTask):
    """A workflowtask that copies and reads the ingest report from preservation
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
                config_object = Configuration(self.config)
                metax_client = Metax(config_object.get('metax_url'),
                                     config_object.get('metax_user'),
                                     config_object.get('metax_password'))
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
                    self.__handle_rejected_sip__(ingest_report_paths,
                                                 metax_client)
                else:
                    raise ValueError('Report was found in incorrect.'
                                     ' Path: %s' % ingest_report_paths[0])

    def __handle_rejected_sip__(self, ingest_report_paths, metax_client):
        dataset_metadata = metax_client.get_dataset(self.dataset_id)
        email_address = dataset_metadata['research_dataset']\
            ['rights_holder']['email']
        conf = Configuration(self.config)
        report_file = get_report_file(ingest_report_paths[0],
                                      self.workspace,
                                      conf)
        if os.path.isfile(report_file):
            pass
            # Check Jira TPASPKT-81
            # mail.send(conf.get('tpas_mail_sender'),
            #          email_address,
            #          conf.get('sip_rejected_mail_subject'),
            #          conf.get('sip_rejected_mail_msg').
            #          format(conf.get('tpas_admin_email')), report_file)
        else:
            msg = 'Report file conflict. Path: %s' % ingest_report_paths[0]
            raise ValueError(msg)
        # Raise exception that informs event handler to set Metax
        # preservation state of this dataset to 7 ("Rejected
        # long-term preservation")
        raise InvalidDatasetError("SIP was rejected")


def get_report_file(remote_dir, local_dir, configuration):
    """ Copies a report file from the PAS server.
    :remote_dir directory path where the report file is copied from
    :local_directry directory path to the directory the report file is copied
    to
    :configuration parameters for creating SSH connection to remote server
    :returns full path of the report file or empty string if not found or
    several report files found
    """
    reportfile = ''
    with paramiko.SSHClient() as ssh:
        # Initialize SSH connection to digital preservation server
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(configuration.get('dp_host'),
                    username=configuration.get('dp_user'),
                    key_filename=configuration.get('dp_ssh_key'))
        rawcommand = 'find {path} -name {pattern}'
        command = rawcommand.format(path=remote_dir, pattern='"*.html"')
        stdin, stdout, stderr = ssh.exec_command(command)
        flist = stdout.read().splitlines()
        with ssh.open_sftp() as sftp_client:
            if len(flist) == 1:
                local_file = local_dir + "/" + os.path.basename(flist[0])
                sftp_client.get(flist[0], local_file)
                reportfile = local_dir + "/" + os.path.basename(flist[0])
    return reportfile
