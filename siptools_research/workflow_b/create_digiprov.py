"""Luigi task that creates digital provenance information. Requires
CreateDescriptiveMetadata."""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import touch_file

from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask

from siptools_research.workflow_b.create_dmdsec \
    import CreateDescriptiveMetadata

from siptools_research.utils.scripts import create_digiprov


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """

    # TODO: Why workspace must be defined here? It is aready defined in
    # WorkflowTask baseclass.
    workspace = Parameter()
    home_path = Parameter()

    def requires(self):
        """Requires create dmdSec file task"""
        return CreateDescriptiveMetadata(
            workspace=self.workspace,
            home_path=self.home_path
        )

    def output(self):
        """Outputs task file"""
        return TaskFileTarget(self.workspace, 'create-provenance-information')

    def run(self):
        """Gets file metadata from Metax.
        :returns: None

        """


        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-provenance-information')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        try:
            with open(os.path.join(self.workspace,
                                   'transfers',
                                   'aineisto')) as infile:
                dataset_id = infile.read()
            digiprov_log = os.path.join(self.workspace, 'logs',
                                        ('task-create-provenance-'
                                         'information.log'))

            # TODO: There must a better way to write stdout to "digiprov_log"
            save_stdout = sys.stdout
            log = open(digiprov_log, 'w')
            sys.stdout = log

            create_digiprov.create_premis_event(dataset_id,
                                                sip_creation_path)

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

            # TODO: task output missing? luigi thinks this task has failed




class DigiprovComplete(WorkflowExternalTask):
    """Task that completes after provencance information has been
    created.
    """

    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'create-provenance-information')
