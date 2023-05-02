"""Luigi task that removes files from file cache."""
import os
from luigi import LocalTarget

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_sip import ValidateSIP
from siptools_research.workflow.send_sip import SendSIPToDP


class CleanupFileCache(WorkflowTask):
    """Removes dataset files from file cache.

    Task requires SIP to be sent to digital preservation service and the
    validation to be finished.
    """

    success_message = 'Workspace was cleaned'
    failure_message = 'Cleaning workspace failed'

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return [ValidateSIP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config),
                SendSIPToDP(workspace=self.workspace,
                            dataset_id=self.dataset_id,
                            config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/cleanup-file-cache.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace, 'cleanup-file-cache.finished'
            )
        )

    def _get_identifiers(self):
        """Get file identifiers.

        Return a list of all the file identifiers and the path to the
        downloaded files.

        :returns: Tuple (list of identifiers, cache_path)
        """
        config_object = Configuration(self.config)
        packaging_root = config_object.get("packaging_root")
        cache_path = os.path.join(packaging_root, "file_cache")

        files = self.get_metax_client().get_dataset_files(self.dataset_id)
        return [_file["identifier"] for _file in files], cache_path

    def run(self):
        """Remove cached files."""
        identifiers, cache_path = self._get_identifiers()

        for identifier in identifiers:
            filepath = os.path.join(cache_path, identifier)
            try:
                os.remove(filepath)
            except FileNotFoundError:
                pass

        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
