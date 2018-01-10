"""Luigi task that creates descriptive metadata."""

import os
from luigi import LocalTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools.scripts import import_description


class CreateDescriptiveMetadata(WorkflowTask):
    """Workflow task that creates mets dmdSec from DataCite file.
    """
    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """Workspace must created and Metax metadata must be validated.

        :returns: list of tasks: [CreateWorkspace, ValidateMetadata]
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """Task is ready when ``sip-in-progress/dmdsec.xml`` has been created.

        :returns: LocalTarget
        """
        return LocalTarget(os.path.join(self.sip_creation_path, 'dmdsec.xml'))

    def run(self):
        """Copies datacite.xml metadatafile from Metax. Creates dmdSec XML from
        datacite.xml using siptools import_description script.

        :returns: None
        """
        dmdsec_log = os.path.join(self.workspace,
                                  "logs",
                                  'create-descriptive-metadata.log')
        with open(dmdsec_log, 'w+') as log:
            with redirect_stdout(log):
                # Get datacite.xml from Metax
                datacite = Metax(self.config).get_datacite(self.dataset_id)

                # Write datacite.xml to file
                datacite_path = os.path.join(self.workspace,
                                             'datacite.xml')
                datacite.write(datacite_path)

                # Create dmdsec.xml file
                import_description.main([datacite_path,
                                         '--workspace',
                                         self.sip_creation_path])
