"""Luigi task that gets files from Ida."""

import os
from siptools_research.utils import ida
from siptools_research.utils import metax
from siptools_research.utils import database
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace


class GetFiles(WorkflowTask):
    """A task that reads file metadata from Metax and downloads requred files
    from Ida.
    """

    def requires(self):
        """Returns list of required tasks. This task requires task:
        CreateWorkspace.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        """Returns output target. This task reports to mongodb when task is
        succesfully completed.

        :returns: MongoTaskResult
        """
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Reads list of required files from Metax and downloads them from Ida.
        Adds result report to mongodb.

        :returns: None
        """

        # Find file identifiers from Metax dataset metadata.
        metax_client = metax.Metax()
        dataset_metadata = metax_client.get_data('datasets',
                                                 str(self.dataset_id))
        # get values for filecategory from elasticsearch
        categories = metax_client.get_elasticsearchdata('')
        # get files form ida and create directory structure for files based on
        # filecategories
        try:
            get_files(self, dataset_metadata['research_dataset']['files'],
                      metax_client, categories)
        except KeyError:
            pass
        # read metax directories in ida and get these files into directory
        # structure based on directrory categories
        try:
            get_files(self,
                      dataset_metadata['research_dataset']['directories'],
                      metax_client)

        except KeyError:
            pass

        # Add task report to mongodb
        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')


def get_files(self, dataset_metadata, metax_client, categories):
    """Reads files from IDA and writes them on a path based on use_category in
    Metax
    """
    for file_section in dataset_metadata:
        metax_file_id = file_section['identifier']

        # The path to file in logical structmap is stored in 'use_category' in
        # metax
        filecategorykey = file_section['use_category']['identifier'].strip('/')
        filecategory = get_category(categories['hits']['hits'],
                                    filecategorykey)
        file_path = os.path.join(self.workspace,
                                 'sip-in-progress',
                                 filecategory)

        # Name of the file comes from file metadata
        file_metadata = metax_client.get_data('files', metax_file_id)
        filename = file_metadata['file_name']

        # Ida file id comes from file metadata
        ida_file_id = file_metadata['identifier']

        # Download file from Ida to 'sip-in-progress' directory in workspace
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        ida.download_file(ida_file_id,
                          os.path.join(file_path, filename))


def get_category(categories, filecategorykey):
    """Looks for a value from list of dictionaries. Returns the value of key
    "label" from last dictionary where the value is found.

    :categories: list of dictionaries
    :filecategorykey: value to look for from dictionaries"""
    label = ''
    for hits in categories:
        found = False
        for key, value in hits['_source'].iteritems():
            if value == filecategorykey:
                found = True
                break
        if found:
            for key, value in hits['_source'].iteritems():
                if key == 'label':
                    label = value['en']
                    break

    return label
