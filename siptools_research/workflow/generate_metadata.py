"""Luigi task that generates technical metadata."""

from luigi import LocalTarget
from metax_access import DS_STATE_TECHNICAL_METADATA_GENERATED

from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflowtask import WorkflowTask


class GenerateMetadata(WorkflowTask):
    """Task that generates technical metadata.

    A false target file `generate-metadata.finished` is created into
    preservation workspace directory to notify luigi that this task has
    finished.
    """

    success_message = "Metadata generated"
    failure_message = "Metadata generation failed"

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/metadata_generation/`
                  `generate-metadata.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.metadata_generation_workspace
                / 'generate-metadata.finished'),
        )

    def run(self):
        """Generate technical metadata for files.

        :returns: ``None``
        """
        generate_metadata(self.dataset_id, self.config)

        self.dataset.set_preservation_state(
            DS_STATE_TECHNICAL_METADATA_GENERATED,
            'Metadata generated'
        )

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
