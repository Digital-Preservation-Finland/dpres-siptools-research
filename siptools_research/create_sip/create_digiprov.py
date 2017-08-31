"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils import touch_file

from siptools_research.workflow.task import WorkflowTask, WorkflowExternalTask

from siptools_research.create_sip.create_dmdsec \
    import CreateDescriptiveMetadata

from siptools_research.scripts.create_digiprov_sahke2 import main


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.

    """
    workspace = Parameter()
    sip_creation_path = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires create dmdSec file task"""
        return {"Create descriptive metadata":
                CreateDescriptiveMetadata(
                    workspace=self.workspace,
                    sip_creation_path=self.sip_creation_path,
                    home_path=self.home_path)}

    def output(self):
        """Outputs task file"""
        return TaskFileTarget(self.workspace, 'create-provenance-information')

    def run(self):
        """Creates two provenance files about the EAD3 creation event,
        an event file and an agent file. If unsuccessful writes an error
        message into mongoDB, updates the status of the document and
        rejects the package. The rejected package is moved to the
        users /home/rejected directory.

        :returns: None

        """
        ead3_location = os.path.join(self.sip_creation_path, 'sahke2-ead3.xml')

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-provenance-information')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            digiprov_log = os.path.join(self.workspace, 'logs',
                                        ('task-create-provenance-'
                                         'information.log'))
            save_stdout = sys.stdout
            log = open(digiprov_log, 'w')
            sys.stdout = log

            main([ead3_location, '--workspace', self.sip_creation_path])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': "Provenance metadata created."
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace,
                                      'create-provenance-information'))

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
                outfile.write('Task create-digiprov failed.')

            yield MoveSipToUser(
                workspace=self.workspace,
                home_path=self.home_path)


class DigiprovComplete(WorkflowExternalTask):
    """Task that completes after provencance information has been
    created.
    """

    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'create-provenance-information')
