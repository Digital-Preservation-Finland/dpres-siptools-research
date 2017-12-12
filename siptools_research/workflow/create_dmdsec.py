"""Luigi task that creates descriptive metadata."""

import os
from luigi import LocalTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools.scripts import import_description


class CreateDescriptiveMetadata(WorkflowTask):
    """Workflow task that creates mets dmdSec from DataCite file.
    """
    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """Return required tasks. Workspace must be created.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Returns task output. Task is ready when dmdsec.xml has been created

        :returns: LocalTarget
        """
        return LocalTarget(os.path.join(self.sip_creation_path, 'dmdsec.xml'))

    def run(self):
        """
        Creates a METS dmdSec file from existing datacite.xml file.

        :returns: None
        """
        # TODO: Getting datacite.xml from Metax is not implemented
        datacite_path = os.path.join(self.sip_creation_path, 'datacite.xml')
        dmdsec_log = os.path.join(self.workspace,
                                  "logs",
                                  'create-descriptive-metadata.log')
        with open(dmdsec_log, 'w+') as log:
            with redirect_stdout(log):
                import_description.main([datacite_path,
                                         '--workspace',
                                         self.sip_creation_path])
