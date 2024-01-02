"""Luigi task that creates descriptive metadata."""
import os
import shutil

from tempfile import TemporaryDirectory

import luigi

from siptools.scripts import import_description

from siptools_research.config import Configuration
from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata


class CreateDescriptiveMetadata(WorkflowTask):
    """Create METS dmdSec document.

    Descriptive metadata is read from Metax in DataCite format.
    Descriptive metadata is written to <sip_creation_path>/dmdsec.xml.
    Descriptive metadata references are written to
    <sip_creation_path>/import-description-md-references.jsonl.
    Premis event is written to
    <sip_creation_path>/<event_identifier>-PREMIS%3AEVENT-amd.xml.
    Premis event reference is written to
    `<workspace>/preservation/create-descriptive-metadata.jsonl`.

    Task requires that dataset metadata is validated.
    """

    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return ValidateMetadata(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        """List the output targets of this Task.

        :returns: `<workspace>/preservation/create-descriptive-metadata.jsonl`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(str(self.dataset.preservation_workspace
                                     / 'create-descriptive-metadata.jsonl'))

    def run(self):
        """Copy datacite.xml metadatafile from Metax.

        Creates a METS document that contains dmdSec element with
        datacite metadata.

        :returns: ``None``
        """
        # Get datacite.xml from Metax
        metax_client = get_metax_client(self.config)
        dataset = metax_client.get_dataset(self.dataset_id)
        datacite = metax_client.get_datacite(dataset['identifier'])

        # Write datacite.xml to file
        datacite_path \
            = str(self.dataset.preservation_workspace / 'datacite.xml')
        datacite.write(datacite_path)

        config_object = Configuration(self.config)
        tmp = os.path.join(config_object.get('packaging_root'), 'tmp/')
        with TemporaryDirectory(prefix=tmp) as temporary_workspace:
            # Create output files with siptools
            import_description.import_description(
                dmdsec_location=datacite_path,
                workspace=temporary_workspace,
                without_uuid=True
            )

            # Move created files to SIP creation directory. PREMIS event
            # reference file is moved to output target path after
            # everything else is done.
            with self.output().temporary_path() as target_path:
                shutil.move(os.path.join(temporary_workspace,
                                         'premis-event-md-references.jsonl'),
                            target_path)
                for file_ in os.listdir(temporary_workspace):
                    shutil.move(os.path.join(temporary_workspace, file_),
                                self.dataset.sip_creation_path)
