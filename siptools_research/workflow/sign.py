"""Luigi task that signs METS file"""

import os
import luigi
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_mets import CreateMets
from siptools_research.utils import contextmanager
from siptools.scripts import sign_mets


class SignSIP(WorkflowTask):
    """Task that signs METS file. Task requires METS file to be created."""

    success_message = "Signing SIP completed succesfully"
    failure_message = "Could not sign SIP"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: CreateMets task
        """
        return CreateMets(workspace=self.workspace,
                          dataset_id=self.dataset_id,
                          config=self.config)

    def output(self):
        """The output that this Task produces.

        :returns: local target: `sip-in-progress/signature.sig`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.sip_creation_path,
                                              "signature.sig"))

    def run(self):
        """Signs METS file using sign_mets from siptools.

        :returns: ``None``
        """
        log_path = os.path.join(self.workspace, 'logs', 'task-sign-sip.log')
        with open(log_path, 'w+') as log:
            with contextmanager.redirect_stdout(log):
                sign_mets.main([
                    "--workspace",
                    self.sip_creation_path,
                    Configuration(self.config).get("sip_sign_key")
                ])
