# encoding=utf8

import os
import sys
import traceback

import lxml.etree as ET
from datetime import datetime
from luigi import Parameter

from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget, TaskLogTarget
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.utils.utils import  touch_file
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils import database
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_structmap import CreateStructMap

from siptools.scripts.compile_mets import main


class CreateMets(WorkflowTask):
    """Compile METS document
    """

    def requires(self):
        """Requires METS structMap and METS fileSec"""
        return {"Create StructMap":
                CreateStructMap(workspace=self.workspace)}

    def output(self):
        """Outputs a task file"""
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Compiles all metadata files into METS document.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. 

        :returns: None

        """
        document_id = os.path.basename(self.workspace)
        task_result = None
        task_messages = None
        mets_log = TaskLogTarget(self.workspace,
                                     'create-mets')
        # Redirect stdout to logfile
        with mets_log.open('w') as log:
            with redirect_stdout(log):        
                try:
                    metadata = Metax().get_data('datasets', self.dataset_id)
                    contract_id = metadata["contract"]["id"]
                    main(['--workspace', self.workspace,
                          'tpas', 'tpas', '--clean']) #, --contract_id, contract_id ])
                    task_result = 'success'
                    task_messages = "Mets dodument created."

                except KeyError as ex:
                    task_result = 'failure'
                    task_messages = 'Could not compile mets %s '\
                                    % ex.message

                finally:
           
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Compilation of mets document "\
                                        "failed due to unknown error."
                    database.add_event(self.document_id,
                                       self.task_name,
                                       task_result,
                                       task_messages)
