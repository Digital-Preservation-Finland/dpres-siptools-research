"""Luigi task that generates technical metadata."""

import os
from luigi import LocalTarget
from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflowtask import WorkflowTask


class GenerateMetadata(WorkflowTask):
    """Task that generates technical metadata.

    A false target file `generate-metadata.finished` is created into
    workspace directory to notify luigi that this task has finished.

    Task requires workspace directory to be created.
    """

    success_message = "Metadata generated"
    failure_message = "Metadata generation failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/generate-metadata.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(self.workspace, 'generate-metadata.finished'),
        )

    def run(self):
        """Generate technical metadata for files.

        :returns: ``None``
        """
        generate_metadata(self.dataset_id, self.config)

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
