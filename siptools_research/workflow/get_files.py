"""Luigi task that gets files from Ida."""

import os
import logging
from json import dumps
import luigi
from siptools_research.utils import ida
from siptools_research.utils import metax
from siptools_research.utils import contextmanager
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata

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
                metax_client = metax.Metax(self.config)
                dataset_metadata = metax_client.get_data('datasets',
                                                         str(self.dataset_id))
                dataset_files = metax_client.get_data(
                    'datasets',
                    str(self.dataset_id)+"/files"
                )
                # get values for filecategory from elasticsearch
                categories = metax_client.get_elasticsearchdata()
                # get files from ida and create directory structure for files
                # based on filecategories
                get_files(self, dataset_metadata['research_dataset'],
                          dataset_files, metax_client, categories)


def get_files(self, dataset_metadata, dataset_files, metax_client, categories):
    """Reads files from IDA and writes them on a path based on use_category in
    Metax
    """
    locical_struct = dict()
    for dataset_file in dataset_files:

        file_id = dataset_file['identifier']
        logging.debug("Creating structmap mapping for file: %s", file_id)

        # Get file's use category. The path to the file in logical structmap
        # is stored in 'use_category' in metax.
        filecategory = None
        for file_ in dataset_metadata['files']:
            if file_id == file_['identifier']:
                filecategory = file_['use_category']['pref_label']['en']
                break

        # If file listed in datasets/<id>/files is not listed in 'files'
        # section of dataset metadata, look for parent_directory of the file
        # from  'directories' section. The "use_category" of file is the
        # "use_category" of the parent directory.
        if filecategory is None:
            file_directory = dataset_file['parent_directory']['identifier']
            for directory in dataset_metadata['directories']:
                if file_directory == directory['identifier']:
                    filecategory = directory['use_category']['pref_label']\
                                   ['en']
                    break

        # Get filename and path for file
        filename = dataset_file['file_name']
        path = dataset_file['file_path']

        # Append path to logical_struct[filecategory] list. Create list if it
        # does not exist
        if filecategory not in locical_struct.keys():
            locical_struct[filecategory] = []
        locical_struct[filecategory].append(path)

        # Remove leading '/' from 'path'
        if path.startswith('/'):
            path = path[1:len(path)]

        # Target path for downloaded file
        file_path = os.path.join(self.workspace, 'sip-in-progress', path)

        # Download file from Ida to 'sip-in-progress' directory in workspace.
        # Dataset directory structure is the same as in IDA.
        folder_path = file_path
        if filename in folder_path:
            # Remove filename from folder_path
            folder_path = file_path[:file_path.index(filename)]
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        logging.debug("Fetching file from Ida: %s", file_id)

        # Download file to file_path
        ida.download_file(file_id, file_path,
                          self.config)

    with open(os.path.join(self.workspace,
                           'sip-in-progress',
                           'logical_struct'), 'w') as new_file:
        new_file.write(dumps(locical_struct))
