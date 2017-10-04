# encoding=utf8
"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback
import datetime

import lxml.etree as ET

from luigi import Parameter

from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow_c.create_structmap import CreateStructMap

from siptools.scripts.compile_mets import main


class CreateMets(WorkflowTask):
    """Compile METS document
    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires METS structMap and METS fileSec"""
        return {"Create StructMap":
                CreateStructMap(workspace=self.workspace,
                                sip_creation_path=self.sip_creation_path,
                                home_path=self.home_path)}

    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-mets')

    def run(self):
        """Compiles all metadata files into METS document.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """

        mets_objid = get_s2_nativeid(os.path.join(self.sip_creation_path,
                                                  'sahke2.xml'))

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.create-mets')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            mets_log = os.path.join(self.workspace, 'logs',
                                    'task-create-mets.log')
            save_stdout = sys.stdout
            log = open(mets_log, 'w')
            sys.stdout = log

            main(['--objid', mets_objid,
                  '--workspace', self.sip_creation_path,
                  'kdk', 'Kansallisarkisto', '--clean'])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': "Metadata compiled into METS document."
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace, 'create-mets'))

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
                outfile.write('Task compile-mets failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)


def get_s2_nativeid(s2_file):
    """Gets Native Id from Sahke2 document"""
    tree = ET.parse(s2_file)
    root = tree.getroot()

    nativeid = root.xpath(
        './s2:TransferInformation/s2:NativeId',
        namespaces={'s2':
                    'http://www.arkisto.fi/skeemat/Sahke2/2011/12/20'})[0].text

    return nativeid
