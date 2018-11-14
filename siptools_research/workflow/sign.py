"""Luigi task that signs METS file"""

import os
from luigi import LocalTarget
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_mets import CreateMets
from siptools.scripts import sign_mets
from tempfile import mkdtemp
from shutil import copyfile, move


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
        return LocalTarget(os.path.join(self.sip_creation_path,
                                        "signature.sig"))

    def run(self):
        """Signs METS file using sign_mets from siptools.

        :returns: ``None``
        """
        dirpath = mkdtemp(dir=self.workspace)
        copyfile(os.path.join(self.sip_creation_path, "mets.xml"),
                 os.path.join(dirpath, "mets.xml"))
        sign_mets.main(["--workspace", dirpath,
                        Configuration(self.config).get("sip_sign_key")])
        move(os.path.join(dirpath, "signature.sig"), self.output().path)
