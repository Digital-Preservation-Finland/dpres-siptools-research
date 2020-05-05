"""Luigi task that creates descriptive metadata."""
import os
from uuid import uuid4

import luigi

from metax_access import Metax
from siptools.scripts import import_description
from siptools.utils import remove_dmdsec_references

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata


class CreateDescriptiveMetadata(WorkflowTask):
    """Creates METS dmdSec document. Descriptive metadata is read from Metax in
    DataCite format. Output file is written to <sip_creation_path>/dmdsec.xml.
    Metadata references are written:
    <sip_creation_path>/import-description-md-references.jsonl.

    Task requires that workspace is created and dataset metadata is validated.
    """
    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of tasks: CreateWorkspace and ValidateMetadata
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """The output that this Task produces.

        :returns: list of local targets
        :rtype: LocalTarget
        """
        return [
            luigi.LocalTarget(
                os.path.join(self.sip_creation_path, 'dmdsec.xml'),
                format=luigi.format.Nop
            ),
            luigi.LocalTarget(
                os.path.join(self.sip_creation_path,
                             'import-description-md-references.jsonl'),
                format=luigi.format.Nop
            )
        ]

    def run(self):
        """Copies datacite.xml metadatafile from Metax. Creates a METS document
        that contains dmdSec element with datacite metadata.

        :returns: ``None``
        """
        # Get datacite.xml from Metax
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        dataset = metax_client.get_dataset(self.dataset_id)
        datacite = metax_client.get_datacite(dataset['identifier'])

        # Write datacite.xml to file
        datacite_path = os.path.join(self.workspace,
                                     'datacite.xml')
        datacite.write(datacite_path)

        # Clean up possible dmdSec references created by earlier runs
        remove_dmdsec_references(self.sip_creation_path)

        # Create METS dmdSec element tree that contains datacite, and write it
        dmd_id = '_' + str(uuid4())
        _mets = import_description.create_mets(datacite_path, dmd_id)
        creator = import_description.DmdCreator(self.sip_creation_path)
        creator.write_dmd_ref(_mets, dmd_id, '.')

        with self.output()[0].open('wb') as outputfile:
            _mets.write(outputfile,
                        pretty_print=True,
                        xml_declaration=True,
                        encoding='UTF-8')
