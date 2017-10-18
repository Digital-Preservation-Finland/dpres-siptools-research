"""Luigi task that creates descriptive metadata."""

import os
from datetime import datetime
from luigi import Parameter, IntParameter, LocalTarget
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget, \
    TaskLogTarget
from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask
from siptools_research.luigi.utils import file_age
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.utils import touch_file
from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools.scripts import import_description


class CreateDescriptiveMetadata(WorkflowTask):
    """Create mets dmdSec from DataCite file.
    """

    def requires(self):
        """Return required tasks.
        """
        return ReadyForThis(workspace=self.workspace, min_age=0)


    def output(self):
        """Outputs a task file"""
        return TaskFileTarget(self.workspace, 'create-descriptive-metadata')

    def run(self):
        """
        Creates a METS dmdSec file from existing datacite.xml file. If
        unsuccessful writes an error message into mongoDB, updates the status
        of the document, and rejects the package.

        :returns: None

        """

        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        # TODO: Getting datacite.xml from Metax is not implemented
        datacite_path = os.path.join(sip_creation_path, 'datacite.xml')

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id,
                                   'wf_tasks.create-descriptive-metadata')
        mongo_status = MongoDBTarget(document_id, 'status')
        mongo_timestamp = MongoDBTarget(document_id, 'timestamp')

        dmdsec_log = TaskLogTarget(self.workspace,
                                   'create-descriptive-metadata.log')

        try:
            with dmdsec_log.open('w') as log:
                with redirect_stdout(log):
                    import_description.main([datacite_path,
                                             '--workspace', sip_creation_path])

            task_result = 'success'
            task_messages = "DataCite metadata wrapped into METS descriptive "\
                            "metadata section."

            # task output
            touch_file(TaskFileTarget(self.workspace,
                                      'create-descriptive-metadata'))

        except Exception as exc:
            task_result = 'failure'
            task_messages = exc.message

            mongo_status.write('rejected')

        finally:
            if not task_result:
                task_result = 'failure'
                task_messages = "Creation of provenance metadata "\
                                "failed due to unknown error."

            mongo_task.write(task_result)
            mongo_timestamp.write(datetime.utcnow().isoformat())
            mongo_task.write(
                {
                    'timestamp': datetime.utcnow().isoformat(),
                    'result': task_result,
                    'messages': task_messages
                }
            )
