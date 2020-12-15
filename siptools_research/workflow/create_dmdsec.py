"""Luigi task that creates descriptive metadata."""
import os
import shutil
try:
    from tempfile import TemporaryDirectory
except ImportError:
    # TODO: Remove this when Python 2 support can be dropped
    from siptools_research.temporarydirectory import TemporaryDirectory

import luigi

from metax_access import Metax
from siptools.scripts import import_description

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
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
    `<workspace>/create-descriptive-metadata.jsonl`.

    Task requires that workspace is created and dataset metadata is
    validated.
    """

    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of tasks: CreateWorkspace and ValidateMetadata
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """List the output targets of this Task.

        :returns: output target
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(
            self.workspace,
            'create-descriptive-metadata.jsonl'
        ))

    def run(self):
        """Copy datacite.xml metadatafile from Metax.

        Creates a METS document that contains dmdSec element with
        datacite metadata.

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
                                self.sip_creation_path)

