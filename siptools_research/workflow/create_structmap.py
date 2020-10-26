# encoding=utf8
"""Luigi task that creates fileSec and physical structure map."""

import json
import os

import luigi.format
from luigi import LocalTarget
from siptools.scripts import compile_structmap
from siptools.utils import read_md_references
from xml_helpers.utils import serialize

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

        :returns: `sip-in-progress/filesec.xml` and
                  `sip-in-progress/structmap.xml`
        :rtype: list of local targets
        """
        return [
            LocalTarget(
                os.path.join(self.sip_creation_path, 'filesec.xml'),
                format=luigi.format.Nop
            ),
            LocalTarget(
                os.path.join(self.sip_creation_path, 'structmap.xml'),
                format=luigi.format.Nop
            )
        ]

    def run(self):
        """Create structural map.

        Creates METS fileSec element based on contents of
        `sip-in-progress` directory and writes it to METS document
        `filesec.xml`. FileSec element is used to create physical
        structure map which is written to METS document `structmap.xml`.

        :returns: ``None``
        """
        # Merge premis event reference files
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
        with open(os.path.join(self.sip_creation_path,
                               'premis-event-md-references.jsonl'), 'w') \
                as references:
            references.write(json.dumps({".": {"path_type": "directory",
                                               "streams": {},
                                               "md_ids": md_ids}}))

        # Create fileSec
        attributes = compile_structmap.get_reference_lists(
            workspace=self.sip_creation_path
        )
        # Setup required reference list and supplementary files information.
        (all_amd_refs,
         all_dmd_refs,
         object_refs,
         filelist,
         file_properties) = compile_structmap.get_reference_lists(
            workspace=self.sip_creation_path)
        (supplementary_files,
         supplementary_types) = compile_structmap.iter_supplementary(
            file_properties=file_properties)

        # Create fileSec
        (filesec, file_ids) = compile_structmap.create_filesec(
            all_amd_refs=all_amd_refs,
            object_refs=object_refs,
            file_properties=file_properties,
            supplementary_files=supplementary_files,
            supplementary_types=supplementary_types)
        with self.output()[0].open('wb') as filesecxml:
            filesecxml.write(serialize(filesec))

        # Create physical structmap
        structmap = compile_structmap.create_structmap(
            filesec=filesec,
            structmap_type='Fairdata-physical',
            file_ids=file_ids,
            all_amd_refs=all_amd_refs,
            all_dmd_refs=all_dmd_refs,
            filelist=filelist,
            supplementary_files=supplementary_files,
            supplementary_types=supplementary_types,
            file_properties=file_properties,
            workspace=self.sip_creation_path
        )
        with self.output()[1].open('wb') as structmapxml:
            structmap.write(structmapxml,
                            pretty_print=True,
                            xml_declaration=True,
                            encoding='UTF-8')
