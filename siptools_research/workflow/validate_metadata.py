"""Luigi task that validates metadata provided by Metax."""

import os
from luigi import LocalTarget
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.validate_metadata import validate_metadata
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflowtask import WorkflowTask


class ValidateMetadata(WorkflowTask):
    """Gets metadata from Metax and validates it. Requires workspace directory
    to be created. Writes log to ``logs/validate-metadata.log``.
    """

    def __init__(self, *args, **kwargs):
        super(ValidateMetadata, self).__init__(*args, **kwargs)
        self.metax_client = Metax(self.config)

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
