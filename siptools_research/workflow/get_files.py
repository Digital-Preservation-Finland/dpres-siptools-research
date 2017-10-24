"""Luigi task that gets files from Ida."""

import os
from siptools_research.utils import ida
from siptools_research.utils import metax
from siptools_research.utils import database
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace


class GetFiles(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """

    def requires(self):
        """Required task: CreateWorkspace"""
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        """Adds report to mongodb when task is succesfully completed."""
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Gets files from Ida. Adds result report to mongodb.

        :returns: None
        """

        # Find file identifiers from Metax dataset metadata.
        metax_client = metax.Metax()
        dataset_metadata = metax_client.get_data('datasets',
                                                 str(self.dataset_id))
        for file_section in dataset_metadata['research_dataset']['files']:
            file_id = file_section['identifier']

            # The path where file should be written in SIP is stored in 'type'
            # field in datasets files section
            file_path = os.path.join(
                self.workspace,
                'sip-in-progress',
                file_section['type']['label']['default'].strip('/')
            )

            # Name of the file comes from file metadata
            file_metadata = metax_client.get_data('files', file_id)
            filename = file_metadata['file_name']

            # Download file from Ida to 'sip-in-progress' directory in workspace
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            ida.download_file(file_id,
                              os.path.join(file_path, filename))

        # Add task report to mongodb
        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')
