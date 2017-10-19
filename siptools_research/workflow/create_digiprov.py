"""Luigi task that creates digital provenance information. Requires
CreateDescriptiveMetadata."""

import os
from siptools.scripts import premis_event
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_dmdsec \
    import CreateDescriptiveMetadata
from siptools_research.utils import database


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """

    def requires(self):
        """Requires create dmdSec file task"""
        return CreateDescriptiveMetadata(
            workspace=self.workspace,
            dataset_id=self.dataset_id
        )

    def output(self):
        """Outputs task file"""
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Gets file metadata from Metax.
        :returns: None
        """
        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        digiprov_log = os.path.join(self.workspace,
                                    'logs',
                                    'task-create-provenance-information.log')

        # Redirect stdout to logfile
        with open(digiprov_log, 'w') as log:
            with redirect_stdout(log):

                try:
                    create_premis_event(self.dataset_id, sip_creation_path)
                    task_result = 'success'
                    task_messages = "Provenance metadata created."
                except KeyError as exc:
                    task_result = 'failure'
                    task_messages = 'Could not create procenance metada, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message

                    database.set_status(self.document_id, 'rejected')

                finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Creation of provenance metadata "\
                                        "failed due to unknown error."

                    database.add_event(self.document_id,
                                       self.task_name,
                                       task_result,
                                       task_messages)


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
