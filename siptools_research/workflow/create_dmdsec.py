"""Luigi task that creates descriptive metadata."""

from siptools.scripts import import_description

from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask


class CreateDescriptiveMetadata(WorkflowTask):

    def run(self):
        """Create METS dmdSec document.

        Descriptive metadata is read from Metax in DataCite format.
        Descriptive metadata is written to <sip_creation_path>/dmdsec.xml.
        Descriptive metadata references are written to
        <sip_creation_path>/import-description-md-references.jsonl.
        Premis event is written to
        <sip_creation_path>/<event_identifier>-PREMIS%3AEVENT-amd.xml.
        Premis event reference is written to
        <sip_creation_path>/premis-event-md-references.jsonl.
        """
        # Get datacite.xml from Metax
        metax_client = get_metax_client(self.config)
        dataset = metax_client.get_dataset(self.dataset_id)
        datacite = metax_client.get_datacite(dataset['identifier'])

        # Write datacite.xml to file
        datacite_path \
            = str(self.dataset.preservation_workspace / 'datacite.xml')
        datacite.write(datacite_path)

        # Create output files with siptools
        import_description.import_description(
            dmdsec_location=datacite_path,
            workspace=self.dataset.sip_creation_path,
            without_uuid=True
        )
