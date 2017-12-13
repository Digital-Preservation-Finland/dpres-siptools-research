"""Luigi task that signs METS file"""

import os
import luigi
from siptools_research.config import Configuration
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_mets import CreateMets
from siptools_research.utils import contextmanager
from siptools.scripts import sign_mets


class SignSIP(WorkflowTask):
    """Task that signs METS file."""
    success_message = "Signing SIP completed succesfully"
    failure_message = "Could not sign SIP"

    def requires(self):
        """Returns required tasks.

        :returns: CreateMets task
        """
        return CreateMets(workspace=self.workspace,
                          dataset_id=self.dataset_id,
                          config=self.config)

    def output(self):
        """Outputs signature file: ``sip-in-progress/signature.sig``.

        :returns: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.sip_creation_path,
                                              "signature.sig"))

    def run(self):
        """Signs METS file using sign_mets from siptools.

        :returns: None
        """
        mets_location = 'mets.xml'
        signature_file = os.path.join(self.sip_creation_path, 'signature.sig')

        log_path = os.path.join(self.workspace, 'logs', 'task-sign-sip.log')
        with open(log_path, 'w+') as log:
            with contextmanager.redirect_stdout(log):
                sign_mets.main([
                    mets_location,
                    signature_file,
                    Configuration(self.config).get("sip_sign_key")
                ])
