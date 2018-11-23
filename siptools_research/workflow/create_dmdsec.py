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
    """Workflow task that creates mets dmdSec from DataCite file. Task requires
    that workspace is created and dataset metadata is validated.
    """
    success_message = "Descriptive metadata created"
    failure_message = "Creating descriptive metadata failed"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of tasks: [CreateWorkspace, ValidateMetadata]
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """The output that this Task produces.

        A false target ``create-descriptive-metadata.finished`` is created into
        workspace directory to notify luigi (and dependent tasks) that this
        task has finished.

        :returns: list of local targets: ``sip-in-progress/dmdsec.xml`` and
            ``create-descriptive-metadata.finished``
        :rtype: LocalTarget
        """
        targets = []
        targets.append(LocalTarget(os.path.join(self.sip_creation_path,
                                                'dmdsec.xml')))
        targets.append(LocalTarget(os.path.join(self.workspace,
                                                'create-descriptive-metadata.'
                                                'finished')))
        return targets

    def run(self):
        """Copies datacite.xml metadatafile from Metax. Creates dmdSec XML from
        datacite.xml using siptools import_description script.

        :returns: None
        """
        # Get datacite.xml from Metax
        config_object = Configuration(self.config)
        datacite = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password')
        ).get_datacite(self.dataset_id)

        # Write datacite.xml to file
        datacite_path = os.path.join(self.workspace,
                                     'datacite.xml')
        datacite.write(datacite_path)

        # Create dmdsec.xml file
        import_description.main([datacite_path,
                                 '--workspace',
                                 self.sip_creation_path])
        with self.output()[-1].open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
