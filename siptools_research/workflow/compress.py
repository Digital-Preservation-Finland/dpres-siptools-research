"""required tasks to validate SIPs and create AIPs"""

import os
import tarfile
import luigi
from siptools_research.luigi.task import WorkflowTask
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.workflow.sign import SignSIP


class CompressSIP(WorkflowTask):
    """Compresses contents to a tar file as SIP.

    :path: workspace/created_sip
    """
    success_message = "SIP was compressed"
    failure_message = "Compressing SIP failed"

    def requires(self):
        """Requires signature file

        :returns: SignSIP task"""
        return SignSIP(workspace=self.workspace,
                       dataset_id=self.dataset_id,
                       config=self.config)

    def output(self):
        """Returns task output.

        :returns: luigi.LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, self.document_id) + '.tar'
        )

    def run(self):
        """Creates a tar archive file conatining mets.xml, signature.sig and
        all content files.

        :returns: None
        """
        compress_log = os.path.join(self.logs_path, 'task-compress-sip.log')
        # Redirect stdout to logfile
        with open(compress_log, 'w') as log:
            with redirect_stdout(log):
                with tarfile.open(self.output().path, 'w') as tar:
                    tar.add(self.sip_creation_path, arcname='.')
