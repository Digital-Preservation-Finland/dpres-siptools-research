"""Luigi task that validates files."""
from pathlib import Path

from luigi import LocalTarget

from siptools_research.file_validator import validate_files
from siptools_research.workflowtask import WorkflowTask
from siptools_research.tasks.get_files import GetFiles


class ValidateFiles(WorkflowTask):
    """Validates all files of the dataset.

    Requires that files have been downloaded.

    This task does not really produce anything, so a false target file
    `validate-files.finished` is created into validation workspace.
    """

    success_message = "All files are valid"
    failure_message = "Some files are invalid"

    def requires(self):
        return GetFiles(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        return LocalTarget(
            str(self.dataset.validation_workspace / 'validate-files.finished')
        )

    def run(self):
        validate_files(self.dataset_id, Path(self.input().path), self.config)

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
