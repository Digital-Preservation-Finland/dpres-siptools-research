"""Luigi task that validates metadata provided by Metax."""

import os
from luigi import LocalTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.validate_metadata import validate_metadata
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflowtask import InvalidMetadataError


class ValidateMetadata(WorkflowTask):
    """Gets metadata from Metax and validates it. Requires workspace directory
    to be created. Writes log to ``logs/validate-metadata.log``.
    """
    success_message = "Metax metadata is valid"
    failure_message = "Metax metadata could not be validated"

    def requires(self):
        """Requires workspace to be created

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Creates ``logs/validate-metadata.log`` file.

        :returns: LocalTarget
        """
        return LocalTarget(os.path.join(self.logs_path,
                                        'validate-metadata.log'))

    def run(self):
        """Reads dataset metadata, file metadata, and additional XML metadata
        from Metax and validates them against schemas. Stdout is redirected to
        log file.

        :returns: None
        """
        with self.output().open('w') as log:
            with redirect_stdout(log):
                # Validate dataset metadata
                validate_metadata(self.dataset_id, self.config)
