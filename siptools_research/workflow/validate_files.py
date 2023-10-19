"""Luigi task that validates files."""

from luigi import LocalTarget

from siptools_research.file_validator import validate_files
from siptools_research.workflowtask import WorkflowTask


class ValidateFiles(WorkflowTask):
    """Validates all files of dataset.

    A false target file `validate-files.finished` is created into
    workspace directory to notify luigi that this task has finished.
    """

    success_message = "All files are valid"
    failure_message = "Some files are invalid"

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/validation/validate-files.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(str(self.dataset.validation_workspace
                               / 'validate-files.finished'))

    def run(self):
        """Validate dataset files.

        Downloads all files of dataset and validates them.

        :returns: ``None``
        """
        # Validate dataset metadata
        validate_files(self.dataset_id, self.config)

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
