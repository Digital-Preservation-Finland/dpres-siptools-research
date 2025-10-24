"""Task that copies dataset metadata to PAS data catalog."""

from luigi import LocalTarget

from siptools_research.workflow.validate_files import ValidateFiles
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflowtask import WorkflowTask


class CopyToPasDataCatalog(WorkflowTask):
    """Task that copies dataset metadata to PAS data catalog.

    A false target `copy-to-pas-data-catalog.finished` is
    created into workspace directory to notify luigi (and dependent
    tasks) that this task has finished.

    The task requires metadata and files to be validated.
    """

    success_message = "Dataset copied to PAS data catalog"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: ValidateMetadata task
        """
        return [ValidateMetadata(dataset_id=self.dataset_id,
                                 config=self.config),
                ValidateFiles(dataset_id=self.dataset_id,
                              config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/preservation/`
                  `copy-dataset-to-pas-data-catalog.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(str(self.dataset.preservation_workspace
                               / 'copy-dataset-to-pas-data-catalog.finished'))

    def run(self):
        """Copy dataset metadata to PAS data catalog.

        :returns: ``None``
        """
        self.dataset.copy_to_pas_datacatalog()

        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
