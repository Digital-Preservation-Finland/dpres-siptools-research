"""Luigi task that validates metadata provided by Metax."""

import os
from luigi import LocalTarget
from siptools_research.metadata_validator import validate_metadata
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflowtask import WorkflowTask


class ValidateMetadata(WorkflowTask):
    """Reads metadata from Metax and validates it.

    A false target file `validate-metadata.finished` is created into
    workspace directory to notify luigi that this task has finished.

    Task requires workspace directory to be created.
    """

    success_message = "Metax metadata is valid"
    failure_message = "Metax metadata could not be validated"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: local target: `validate-metadata.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(self.workspace, 'validate-metadata.finished'),
        )

    def run(self):
        """Validate dataset metadata.

        Reads dataset metadata, file metadata, and additional XML
        metadata from Metax and validates them against schemas.

        :returns: ``None``
        """
        # Validate dataset metadata
        validate_metadata(self.dataset_id, self.config)

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
