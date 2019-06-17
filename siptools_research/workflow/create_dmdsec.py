"""Luigi task that creates descriptive metadata."""
import os

from luigi import LocalTarget

from metax_access import Metax
from siptools.scripts import import_description

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata


class CreateDescriptiveMetadata(WorkflowTask):
    """Creates METS dmdSec document. Descriptive metadata is read from Metax in
    DataCite format. Output file is written to <sip_creation_path>/dmdsec.xml

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

        :returns: local target: `sip-in-progress/dmdsec.xml`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(self.workspace, 'sip-in-progress/dmdsec.xml')
        )

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

        # Create METS dmdSec file that contains datacite as XML tree
        import_description.import_description(
            datacite_path, workspace=self.sip_creation_path, without_uuid=True
        )
