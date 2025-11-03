"""Luigi task that creates tar-archive from SIP directory."""

import tarfile
import luigi
from siptools_research.workflowtask import WorkflowTask
from siptools_research.tasks.create_mets import CreateMets
from siptools_research.tasks.get_files import GetFiles
from siptools_research.tasks.sign import SignSIP


class CompressSIP(WorkflowTask):
    """Creates tar-archive from SIP directory.

    Outputs `<dataset_id>.tar` to workspace.

    Task requires that SIP has been signed, METS document has been
    created, and dataset files have been downloaded.
    """

    success_message = "TAR archive was created"
    failure_message = "Creating TAR archive failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: dictionary of required tasks
        """
        return {
            'signature': SignSIP(dataset_id=self.dataset_id,
                                 config=self.config),
            'mets': CreateMets(dataset_id=self.dataset_id,
                               config=self.config),
            'files': GetFiles(dataset_id=self.dataset_id,
                              config=self.config),
        }

    def output(self):
        """Return the output target of the Task.

        :returns: `<workspace>/preservation/<dataset_id>.tar`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            str(self.dataset.preservation_workspace
                / self.dataset_id) + '.tar',
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
        with self.output().temporary_path() as target_path:
            with tarfile.open(target_path, 'w') as tar:
                tar.add(self.input()['files'].path, arcname='dataset_files')
                tar.add(self.input()['mets'].path, arcname='mets.xml')
                tar.add(self.input()['signature'].path,
                        arcname='signature.sig')
