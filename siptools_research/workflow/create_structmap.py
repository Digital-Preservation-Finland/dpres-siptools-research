# encoding=utf8
"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback

from datetime import datetime
from luigi import Parameter

from siptools.scripts import compile_structmap

from siptools_research.luigi.target import MongoTaskResultTarget
import siptools_research.utils.database
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata



class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.
    """
    workspace = Parameter()

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                CreateDescriptiveMetadata(workspace=self.workspace,
                                          dataset_id=self.dataset_id),
                "Create provenance information completed":
                CreateProvenanceInformation(workspace=self.workspace,
                                            dataset_id=self.dataset_id),
                "Create technical metadata completed":
                CreateTechnicalMetadata(workspace=self.workspace,
                                        dataset_id=self.dataset_id)}

    def output(self):
        """Outputs a task file"""
        return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name,
                                     config_file=self.config)

    def run(self):

        """Creates a METS structural map file based on a folder structure. Top folder is given as a parameter.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """
        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        document_id = os.path.basename(self.workspace)
        task_result = None

        structmap_log = os.path.join(self.workspace,
                                  "logs",
                                  'create-struct-map')
        # Redirect stdout to logfile
        with open(structmap_log, 'w') as log:
            with redirect_stdout(log):

                try:

                    workspace = self.workspace
                    compile_structmap.main([
                            '--workspace', sip_creation_path])
                    print "stuff3 "
                    task_result = 'success'
                    task_messages = "Structrural map and filesec created."


                except KeyError as ex:
                   task_result = 'failure'
                   task_messages = 'Could not create structrural map, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message


                finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Creation of structmap and filesec "\
                                        "failed due to unknown error."
                    database = siptools_research.utils.database.Database(
                        self.config
                    )
                    database.add_event(self.document_id,
                           self.task_name,
                           task_result,
                           task_messages)

