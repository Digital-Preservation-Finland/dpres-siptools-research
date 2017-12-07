"""Luigi task that gets files from Ida."""

import os
from siptools_research.utils import ida
from siptools_research.utils import metax
import siptools_research.utils.database
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from json import dumps


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
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Returns output target. This task reports to mongodb when task is
        succesfully completed.

        :returns: MongoTaskResult
        """
        return MongoTaskResultTarget(self.document_id, self.task_name,
                                     self.config)

    def run(self):
        """Reads list of required files from Metax and downloads them from Ida.
        Adds result report to mongodb.

        :returns: None
        """

        # Find file identifiers from Metax dataset metadata.
        metax_client = metax.Metax(self.config)
        dataset_metadata = metax_client.get_data('datasets',
                                                 str(self.dataset_id))
        dataset_files = metax_client.get_data('datasets',
                                                  str(self.dataset_id)+"/files")        
        print "mita saaartiiib %s "%dataset_files
        # get values for filecategory from elasticsearch
        categories = metax_client.get_elasticsearchdata()
        # get files from ida and create directory structure for files based on
        # filecategories
        try:
            get_files(self, dataset_metadata['research_dataset']['files'], dataset_files,
                      metax_client, categories)
        except KeyError:
            pass
        # Add task report to mongodb
        database = siptools_research.utils.database.Database(self.config)
        database.add_event(self.document_id,
                           self.task_name,
                           'success',
                           'Workspace directory created')


def get_files(self, dataset_metadata, dataset_files, metax_client, categories):
    """Reads files from IDA and writes them on a path based on use_category in
    Metax
    """
    logicalStruct = dict()
    for file in dataset_files:

        file_id = file['identifier']
        # Get file's use category. The path to the file in logical structmap 
        # is stored in 'use_category' in metax.
        filecategory = None
        for file_section in dataset_metadata:
             metax_file_id = file_section['identifier']
             if file_id == metax_file_id:
                 print "BBB %s " %metax_file_id
                 filecategorykey = file_section['use_category']['identifier'].strip('/')
                 filecategory = get_category(categories['hits']['hits'],
                                    filecategorykey)
                 break
    

        filename = file['file_name']
        path = file['file_path']

        clist = list()
        if logicalStruct:
           if logicalStruct[filecategory] is not None:
               clist = logicalStruct[filecategory]
        print "struct %s" % clist
        clist.append(filename+path)
        print "struct2 %s " %clist        
        logicalStruct[filecategory] = clist
        print "logicalStruct %s" %logicalStruct


        if(path.startswith('/')):
            path = path[1:len(path)]
        file_path = os.path.join(self.workspace, 'sip-in-progress', path)
        print "logical add file %s "%filename
        print " to path %s "% file_path


        # Download file from Ida to 'sip-in-progress' directory in workspace
        # Dataset directory structure is the same as in IDA
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        print "IDA DOWNLOAD %s" %file_path
        ida.download_file(file_id, os.path.join(file_path, filename),
                          self.config)
    with open(os.path.join(self.workspace,
                           'sip-in-progress','logical_struct'), 'w') as new_file:
        new_file.write(dumps(logicalStruct))
        


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
