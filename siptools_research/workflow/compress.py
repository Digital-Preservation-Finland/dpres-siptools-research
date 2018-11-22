"""Luigi task that creates tar-archive from SIP directory"""

import os
import tarfile
import luigi
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.sign import SignSIP


class CompressSIP(WorkflowTask):
    """Creates tar-archive from SIP directory. Task requires that SIP has been
    signed."""
    success_message = "TAR archive was created"
    failure_message = "Creating TAR archive failed"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: SignSIP task
        """
        return SignSIP(workspace=self.workspace,
                       dataset_id=self.dataset_id,
                       config=self.config)

    def output(self):
        """The output that this Task produces.

        :returns: local target: `<workspace>/<document_id>.tar`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, self.document_id) + '.tar'
        )

    def run(self):
        """Creates a tar archive file that contains mets.xml, signature.sig and
        all content files.

        :returns: None
        """
        # To be atomic tar file is written into temp file.
        # On exit luigi moves the temp file to targeted file
        with self.output().temporary_path() as self.temp_output_path:
            with tarfile.open(self.temp_output_path, 'w') as tar:
                tar.add(self.sip_creation_path, arcname='.')
