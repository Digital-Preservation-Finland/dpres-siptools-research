"""Luigi task that generates technical metadata."""

from pathlib import Path

from luigi import LocalTarget
from metax_access import DS_STATE_TECHNICAL_METADATA_GENERATED

from siptools_research.metadata_generator import generate_metadata
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.get_files import GetFiles


class GenerateMetadata(WorkflowTask):
    """Scrapes files and posts metadata to Metax.

    Requires that files have been downloaded.

    The task is complete when every file has metadata in Metax. To avoid
    unnecessary requests to Metax, a false target file
    `generate-metadata.finished` is created into metadata generation
    workspace.
    """

    success_message = "Metadata generated"
    failure_message = "Metadata generation failed"

    def requires(self):
        return GetFiles(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        return LocalTarget(
            str(self.dataset.metadata_generation_workspace
                / 'generate-metadata.finished'),
        )

    def run(self):
        """Generate technical metadata for files.

        :returns: ``None``
        """
        dataset_files = Path(self.input().path)
        generate_metadata(self.dataset_id,
                          root_directory=dataset_files,
                          config=self.config)

        self.dataset.set_preservation_state(
            DS_STATE_TECHNICAL_METADATA_GENERATED,
            'Metadata generated'
        )

        with self.output().open('w') as log:
            log.write('Dataset id=' + self.dataset_id)
