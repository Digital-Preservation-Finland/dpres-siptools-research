"""Luigi task that creates digital provenance information. Requires
CreateDescriptiveMetadata."""

import os
from datetime import datetime
from luigi import Parameter
from siptools.scripts import premis_event
from siptools_research.utils.utils import touch_file
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget, \
    TaskLogTarget
from siptools_research.luigi.task import WorkflowTask, WorkflowExternalTask
from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.workflow.create_dmdsec \
    import CreateDescriptiveMetadata


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
        # Also MongoDBTarget should be returned
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
        task_result = None
        task_messages = None

        # Should the dataset_id be a luigi Parameter?
        with open(os.path.join(self.workspace,
                               'transfers',
                               'aineisto')) as infile:
            dataset_id = infile.read()

        digiprov_log = TaskLogTarget(self.workspace,
                                     'create-provenance-information')

        # Redirect stdout to logfile
        with digiprov_log.open('w') as log:
            with redirect_stdout(log):

                try:
                    create_premis_event(dataset_id, sip_creation_path)

                    task_result = 'success'
                    task_messages = "Provenance metadata created."

                    # task output
                    touch_file(TaskFileTarget(self.workspace,
                                              'create-provenance-information'))
                except KeyError as exc:
                    task_result = 'failure'
                    task_messages = 'Could not create procenance metada, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message

                    failed_log = FailureLog(self.workspace).output()
                    with failed_log.open('w') as outfile:
                        outfile.write('Task create-digiprov failed.')

                    mongo_status.write('rejected')

                    yield MoveSipToUser(
                        workspace=self.workspace,
                        home_path=self.home_path
                    )
                finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Creation of provenance metadata "\
                                        "failed due to unknown error."

                    mongo_timestamp.write(
                        datetime.utcnow().isoformat()
                    )

                    mongo_task.write(
                        {
                            'timestamp': datetime.utcnow().isoformat(),
                            'result': task_result,
                            'messages': task_messages
                        }
                    )


def create_premis_event(dataset_id, workspace):
    """Gets metada from Metax and calls siptools premis_event script."""

    metadata = Metax().get_data('datasets', dataset_id)
    event_type = metadata["research_dataset"]["provenance"][0]["type"]\
        ["pref_label"][0]["en"]
    event_datetime = metadata["research_dataset"]["provenance"][0]\
        ["temporal"][0]["start_date"]
    event_detail = metadata["research_dataset"]["provenance"][0]\
        ["description"]["en"]

    premis_event.main([
        event_type, event_datetime,
        "--event_detail", event_detail,
        "--workspace", workspace
    ])





class DigiprovComplete(WorkflowExternalTask):
    """Task that completes after provencance information has been
    created.
    """

    def output(self):
        """Task output.
        """
        return TaskFileTarget(self.workspace, 'create-provenance-information')
