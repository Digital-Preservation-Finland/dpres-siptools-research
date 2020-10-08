"""Luigi task that creates tar-archive from SIP directory."""

import os
import tarfile
import luigi
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.sign import SignSIP


class CompressSIP(WorkflowTask):
    """Creates tar-archive from SIP directory.

    Outputs `<document_id>.tar` to workspace.

    Task requires that SIP has been signed.
    """

    success_message = "TAR archive was created"
    failure_message = "Creating TAR archive failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: SignSIP task
        """
        return SignSIP(workspace=self.workspace,
                       dataset_id=self.dataset_id,
                       config=self.config)

    def output(self):
        """Return the output target of the Task.

        :returns: local target: `<document_id>.tar`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, self.document_id) + '.tar',
            format=luigi.format.Nop
        )

    def run(self):
        """Collect files to SIP.

        Creates a tar archive file that contains mets.xml, signature.sig
        and all content files.

        :returns: None
        """
        # To be atomic tar file is written into temp file.
        # On exit luigi moves the temp file to targeted file
        with self.output().temporary_path() as temp_output_path:
            with tarfile.open(temp_output_path, 'w') as tar:
                tar.add(self.sip_creation_path, arcname='.')
