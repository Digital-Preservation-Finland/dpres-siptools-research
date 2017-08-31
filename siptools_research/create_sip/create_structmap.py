# encoding=utf8
"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils import touch_file

from siptools_research.workflow.task import WorkflowTask

from siptools_research.create_sip.create_dmdsec import DmdsecComplete
from siptools_research.create_sip.create_digiprov import DigiprovComplete
from siptools_research.create_sip.create_techmd import TechMDComplete

from siptools_research.scripts.compile_ead3_structmap import main


class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                DmdsecComplete(workspace=self.workspace),
                "Create provenance information completed":
                DigiprovComplete(workspace=self.workspace),
                "Create technical metadata completed":
                TechMDComplete(workspace=self.workspace)}

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-structmap')

    def run(self):
        """Creates a METS structural map file from the composition of
        EAD3 strucutral elements and a METS file section file from the
        list of files in the SÄHKE2 files.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """
        s2_name = 'sahke2.xml'
        ead3_location = os.path.join(self.sip_creation_path, 'sahke2-ead3.xml')

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   ('wf_tasks.create-structural-map'
                                    '-and-file-section'))
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            structmap_log = os.path.join(self.workspace, 'logs',
                                         'task-create-struct-map.log')
            save_stdout = sys.stdout
            log = open(structmap_log, 'w')
            sys.stdout = log

            main([self.sip_creation_path, ead3_location, s2_name,
                  '--workspace', self.sip_creation_path, '--clean'])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': "METS structural map and file section created."
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace, 'create-structmap'))

        except Exception as ex:
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'failure',
                'messages': traceback.format_exc()
            }
            mongo_status.write('rejected')
            mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
            mongo_task.write(task_result)

            failed_log = FailureLog(self.workspace).output()
            with failed_log.open('w') as outfile:
                outfile.write('Task create-structmap_filesec failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)
