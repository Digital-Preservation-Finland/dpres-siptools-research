"""Luigi task that removes files from file cache."""
import os
import shutil

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.report_preservation_status \
    import ReportPreservationStatus


class Cleanup(WorkflowTask):
    """Removes workspace and dataset files from file cache.

    Task requires that preservation status has been reported.
    """

    success_message = 'Workspace was cleaned'
    failure_message = 'Cleaning workspace failed'

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return ReportPreservationStatus(dataset_id=self.dataset_id,
                                        config=self.config)

    def complete(self):
        """Check if Task is complete.

        Cleanup is done when workspace directory does not exist.

        :rtype: LocalTarget
        """
        return not self.dataset.workspace_root.exists()

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
        """Remove cached files and workspace."""
        identifiers, cache_path = self._get_identifiers()

        for identifier in identifiers:
            filepath = os.path.join(cache_path, identifier)
            try:
                os.remove(filepath)
            except FileNotFoundError:
                pass

        shutil.rmtree(self.dataset.workspace_root)
