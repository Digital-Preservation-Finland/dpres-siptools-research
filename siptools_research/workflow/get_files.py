"""Luigi task that gets files from Ida."""

import os
import logging
import luigi
from metax_access import Metax
from siptools_research.utils import ida
from siptools_research.utils import contextmanager
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration
from requests.exceptions import HTTPError

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)


class GetFiles(WorkflowTask):
    """A task that reads file metadata from Metax and downloads requred files
    from Ida.
    """
    success_message = 'Files were downloaded from IDA'
    failure_message = 'Could not get files from IDA'

    def requires(self):
        """Requires workspace directory to be created.

        :returns: CreateWorkspace task
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """Outputs log to ``logs/task-getfiles.log``

        :returns: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.logs_path,
                                              "task-getfiles.log"))

    def run(self):
        """Reads list of required files from Metax and downloads them from Ida.

        :returns: None
        """

        with self.output().open('w') as log:
            with contextmanager.redirect_stdout(log):
                # Find file identifiers from Metax dataset metadata.
                config_object = Configuration(self.config)
                metax_client = Metax(config_object.get('metax_url'),
                                     config_object.get('metax_user'),
                                     config_object.get('metax_password'))
                dataset_files = metax_client.get_dataset_files(self.dataset_id)

                # get files from ida
                download_files(self, dataset_files)


def download_files(self, dataset_files):
    """Reads files from IDA and writes them on a path based on use_category in
    Metax
    """
    for dataset_file in dataset_files:

        # Full path to file
        target_path = os.path.join(self.sip_creation_path,
                                   dataset_file['file_path'].strip('/'))

        # Create the download directory for file if it does not exist already
        if not os.path.isdir(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        # Download file
        try:
            ida.download_file(dataset_file['identifier'],
                              target_path,
                              self.config)
        except HTTPError as error:
            file_path = dataset_file['file_path']
            status_code = error.response.status_code
            if status_code == 404:
                raise Exception("File %s not found in Ida." % file_path)
            elif status_code == 403:
                raise Exception("Access to file %s forbidden." % file_path)
            else:
                raise Exception("File %s could not be retrieved." % file_path)
