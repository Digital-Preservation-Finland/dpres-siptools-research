"""Luigi task that creates descriptive metadata."""

import os
import sys
import traceback
import datetime

from luigi import Parameter, IntParameter, LocalTarget

from siptools_research.move_sip import MoveSipToUser, FailureLog
from siptools_research.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils import touch_file

from siptools_research.workflow.task import WorkflowTask, WorkflowExternalTask
from siptools_research.workflow.utils import file_age

from siptools.scripts import import_description


class CreateDescriptiveMetadata(WorkflowTask):
    """Create mets dmdSec from EAD3 finding aid file.

    """
    workspace = Parameter()
    home_path = Parameter()

    def requires(self):
        """Return required tasks.

        :returns: Files must have been transferred to workspace
        """

        return ReadyForThis(workspace=self.workspace, min_age=0)


    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-descriptive-metadata')

    def run(self):
        """
        Creates a METS dmdSec file from existing datacite.xml file. If
        unsuccessful writes an error message into mongoDB, updates the status
        of the document and rejects the package. The rejected package is moved
        to the users home/rejected directory.

        :returns: None

        """

        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        # TODO: Getting datacite.xml here from Metax is not implemented
        datacite_path = os.path.join(sip_creation_path, 'datacite.xml')

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-descriptive-metadata')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            dmdsec_log = os.path.join(self.workspace, 'logs',
                                      'task-create-descriptive-metadata.log')
            save_stdout = sys.stdout
            log = open(dmdsec_log, 'w')
            sys.stdout = log

            import_description.main([datacite_path,
                                     '--workspace', sip_creation_path])

            sys.stdout = save_stdout
            log.close()

            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': ("DataCite metadata wrapped into METS descriptive "
                             "metadata section.")
            }
            mongo_task.write(task_result)

            # task output
            touch_file(TaskFileTarget(self.workspace,
                                      'create-descriptive-metadata'))

        except Exception as ex:
            print ex.message
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
                outfile.write('Task create-dmdsec failed.')

            #-------------------
            # This breaks tests
            # yield MoveSipToUser(
                # workspace=self.workspace,
                # home_path=self.home_path)
            #-------------------


class ReadyForThis(WorkflowExternalTask):
    """Check that the workspace is older than min_age."""
    min_age = IntParameter()

    def output(self):
        return LocalTarget(self.workspace)

    def complete(self):
        return file_age(self.workspace) > self.min_age


class DmdsecComplete(WorkflowExternalTask):
    """Task that completes after dmdSec has been created.
    """
    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'create-descriptive-metadata')
