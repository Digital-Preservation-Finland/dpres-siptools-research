"""Luigi task that creates tar-archive from SIP directory"""

import os
import tarfile
import luigi
from siptools_research.workflowtask import WorkflowTask
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.workflow.sign import SignSIP


class CompressSIP(WorkflowTask):
    """Creates tar-archive from SIP directory. Task requires that SIP has been
    signed.
    """
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
        compress_log = os.path.join(self.logs_path, 'task-compress-sip.log')
        # Redirect stdout to logfile
        with open(compress_log, 'w') as log:
            with redirect_stdout(log):
                with tarfile.open(self.output().path, 'w') as tar:
                    tar.add(self.sip_creation_path, arcname='.')
