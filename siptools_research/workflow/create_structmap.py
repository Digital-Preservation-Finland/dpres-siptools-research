# encoding=utf8
"""Luigi task that creates fileSec and physical structure map."""

import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from luigi import LocalTarget
from siptools.scripts import compile_structmap
from siptools.utils import read_md_references

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata


class CreateStructMap(WorkflowTask):
    """Creates structural map and file section.

    Structural map and file section are written to separate METS
    documents: `<sip_creation_path>/structmap.xml` and
    `<sip_creation_path>/filesec.xml`

    Task requires descriptive metadata, provenance information, and
    technical metadata to be created.
    """

    success_message = "Structure map created"
    failure_message = "Structure map could not be created"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: dictionary of required tasks
        """
        return {
            'create_provenance_information': CreateProvenanceInformation(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            ),
            'create_descriptive_metadata': CreateDescriptiveMetadata(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            ),
            'create_technical_metadata': CreateTechnicalMetadata(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            )
        }

    def output(self):
        """List the output targets of this Task.

        :returns: Local target `sip-in-progress/filesec.xml`
        """
        return LocalTarget(str(Path(self.sip_creation_path) / 'filesec.xml'))

    def run(self):
        """Create structural map.

        Creates METS fileSec element based on contents of
        `sip-in-progress` directory and writes it to METS document
        `filesec.xml`. FileSec element is used to create physical
        structure map which is written to METS document `structmap.xml`.

        :returns: ``None``
        """
        tmp = str(
            Path(Configuration(self.config).get('packaging_root')) / 'tmp'
        )
        with TemporaryDirectory(prefix=tmp) as temporary_directory:
            permanent_workspace = Path(self.sip_creation_path)
            temporary_workspace = Path(temporary_directory)

            # Copy reference files to temporary workspace
            for file in ("import-object-md-references.jsonl",
                         "import-description-md-references.jsonl",
                         "create-addml-md-references.jsonl",
                         "create-audiomd-md-references.jsonl",
                         "create-mix-md-references.jsonl",
                         "create-videomd-md-references.jsonl"):

                source = permanent_workspace / file
                if source.is_file():
                    shutil.copy(source, temporary_workspace / file)

            # Merge premis event reference files into one file that is
            # written to temporary directory
            md_ids = []
            for input_target in ('create_provenance_information',
                                 'create_descriptive_metadata',
                                 'create_technical_metadata'):
                md_ids += (
                    read_md_references(
                        self.workspace,
                        self.input()[input_target].path
                    )['.']['md_ids']
                )
            with (temporary_workspace / 'premis-event-md-references.jsonl') \
                    .open('w') as references:
                references.write(json.dumps({".": {"path_type": "directory",
                                                   "streams": {},
                                                   "md_ids": md_ids}}))

            # Compile fileSec and structure map in temporary workspace
            compile_structmap.compile_structmap(
                workspace=str(temporary_workspace),
                structmap_type='Fairdata-physical',
                stdout=False
            )

            # After successful compilation, move all files to permanent
            # workspace. The output file (filesec.xml) is moved after
            # any other files.
            with self.output().temporary_path() as temporary_path:
                (temporary_workspace / 'filesec.xml').replace(temporary_path)
                for file in temporary_workspace.iterdir():
                    file.replace(permanent_workspace / file.name)
