"""Luigi task that validates metadata provided by Metax."""
from luigi import LocalTarget

from siptools_research.metadata_validator import MetadataValidator
from siptools_research.workflowtask import WorkflowTask


class ValidateMetadata(WorkflowTask):
    """Reads metadata from Metax and validates it.

    A false target file `validate-metadata.finished` is created into
    validation workspace directory to notify luigi that this task has
    finished.
    """

    success_message = "Metax metadata is valid"
    failure_message = "Metax metadata could not be validated"

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/validation/validate-metadata.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(str(self.dataset.validation_workspace
                               / 'validate-metadata.finished'))

    def run(self):
        """Validate dataset metadata.

        Reads dataset metadata, file metadata, and additional XML
        metadata from Metax and validates them against schemas.

        :returns: ``None``
        """
        # Validate dataset metadata
        MetadataValidator(self.dataset_id, self.config).validate()

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
