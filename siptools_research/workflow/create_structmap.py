# encoding=utf8
"""Luigi task that creates fileSec and physical structure map."""

import os

import luigi.format
from luigi import LocalTarget
from siptools.scripts import compile_structmap
from siptools.mdcreator import (get_objectlist, read_md_references,
                                read_all_amd_references)

from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata


class CreateStructMap(WorkflowTask):
    """Create METS documents that contain structural map and file section.
    Files are written to `<sip_creation_path>/structmap.xml` and
    `<sip_creation_path>/filesec.xml`

    Task requires descriptive metadata, provenance information, and technical
    metadata to be created.
    """
    success_message = "Structure map created"
    failure_message = "Structure map could not be created"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of tasks
        """
        return [
            CreateDescriptiveMetadata(workspace=self.workspace,
                                      dataset_id=self.dataset_id,
                                      config=self.config),
            CreateProvenanceInformation(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config),
            CreateTechnicalMetadata(workspace=self.workspace,
                                    dataset_id=self.dataset_id,
                                    config=self.config)
        ]

    def output(self):
        """The output that this Task produces.

        :returns: list of local targets: `sip-in-progress/filesec.xml` and
                  `sip-in-progress/structmap.xml`
        :rtype: LocalTarget
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
        """Creates METS fileSec element based on contents of `sip-in-progress`
        directory and writes it to METS document `filesec.xml`. FileSec element
        is used to create physical structure map which is written to METS
        document `structmap.xml`.

        :returns: ``None``
        """
        ref_lists = compile_structmap.get_reference_lists(
            workspace=self.sip_creation_path
        )
        # Create fileSec
        filesec = compile_structmap.create_filesec(**ref_lists)
        with self.output()[0].open('wb') as filesecxml:
            filesec.write(filesecxml,
                          pretty_print=True,
                          xml_declaration=True,
                          encoding='UTF-8')

        # Create physical structmap
        filesec_element = filesec.getroot()[0]
        structmap = compile_structmap.create_structmap(
            filesec=filesec_element,
            structmap_type='Fairdata-physical',
            **ref_lists
        )
        with self.output()[1].open('wb') as structmapxml:
            structmap.write(structmapxml,
                            pretty_print=True,
                            xml_declaration=True,
                            encoding='UTF-8')
