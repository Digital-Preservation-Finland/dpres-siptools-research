"""Luigi task that signs METS document."""

import luigi.format
from luigi import LocalTarget
import dpres_signature.signature

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_mets import CreateMets


class SignSIP(WorkflowTask):
    """Task that signs METS file.

    Signature is written to `<workspace>/preservation/signature.sig`.

    Task requires METS file to be created.
    """

    success_message = "Signing SIP completed succesfully"
    failure_message = "Could not sign SIP"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CreateMets task
        """
        return CreateMets(dataset_id=self.dataset_id,
                          config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/preservation/signature.sig`
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.preservation_workspace / "signature.sig"),
            format=luigi.format.Nop
        )

    def run(self):
        """Sign METS document.

        :returns: ``None``
        """
        signature = dpres_signature.signature.create_signature(
            self.dataset.preservation_workspace,
            Configuration(self.config).get("sip_sign_key"),
            ['mets.xml']
        )

        with self.output().open('wb') as signature_file:
            signature_file.write(signature)
