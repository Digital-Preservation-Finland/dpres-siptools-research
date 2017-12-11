"""Luigi task that gets files from Ida."""

import os
from json import dumps
import luigi
from siptools_research.utils import ida
from siptools_research.utils import metax
from siptools_research.utils import contextmanager
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace


class GetFiles(WorkflowTask):
    """A task that reads file metadata from Metax and downloads requred files
    from Ida.
    """
    success_message = 'Files were downloaded from IDA'
    failure_message = 'Could not get files from IDA'

    def requires(self):
        """Returns list of required tasks. This task requires task:
        CreateWorkspace.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Returns output target.

        :returns: MongoTaskResult
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
                try:
                    get_files(self, dataset_metadata['research_dataset'],
                              dataset_files, metax_client, categories)
                except KeyError:
                    pass


def get_files(self, dataset_metadata, dataset_files, metax_client, categories):
    """Reads files from IDA and writes them on a path based on use_category in
    Metax
    """
    locical_struct = dict()
    for dataset_file in dataset_files:

        file_id = dataset_file['identifier']
        # Get file's use category. The path to the file in logical structmap
        # is stored in 'use_category' in metax.
        filecategory = None
        for file_section in dataset_metadata['files']:
            metax_file_id = file_section['identifier']
            if file_id == metax_file_id:
                filecategorykey = file_section['use_category']['identifier']\
                    .strip('/')
                filecategory = get_category(categories['hits']['hits'],
                                            filecategorykey)
                break
        if filecategory is None:
            file_folder = dataset_file['parent_directory']['identifier']
            for directory in dataset_metadata['directories']:
                if file_folder == directory['identifier']:
                    categorykey = directory['use_category']['identifier']\
                        .strip('/')
                    filecategory = get_category(categories['hits']['hits'],
                                                categorykey)
                    break
        filename = dataset_file['file_name']
        path = dataset_file['file_path']
        clist = list()
        try:
            if locical_struct[filecategory] is not None:
                clist = locical_struct[filecategory]
        except KeyError:
            print "error"
        clist.append(path)
        locical_struct[filecategory] = clist

        if path.startswith('/'):
            path = path[1:len(path)]
        file_path = os.path.join(self.workspace, 'sip-in-progress', path)

        # Download file from Ida to 'sip-in-progress' directory in workspace
        # Dataset directory structure is the same as in IDA
        folder_path = file_path
        if filename in folder_path:
            folder_path = file_path[:file_path.index(filename)]
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        ida.download_file(file_id, file_path,
                          self.config)

    with open(os.path.join(self.workspace,
                           'sip-in-progress',
                           'logical_struct'), 'w') as new_file:
        new_file.write(dumps(locical_struct))



def get_category(categories, filecategorykey):
    """Looks for a value from list of dictionaries. Returns the value of key
    "label" from last dictionary where the value is found.

    :categories: list of dictionaries
    :filecategorykey: value to look for from dictionaries"""
    for hits in categories:

        if hits['_source']['uri'] == filecategorykey:
            label = hits['_source']['label']['en']
            break

    return label
