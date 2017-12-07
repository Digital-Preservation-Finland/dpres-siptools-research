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
import siptools_research.utils.database


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """

    def requires(self):
        """Requires create dmdSec file task"""
        return CreateDescriptiveMetadata(
            workspace=self.workspace,
            dataset_id=self.dataset_id,
            config=self.config
        )

    def output(self):
        """Outputs task file"""
        return MongoTaskResultTarget(self.document_id, self.task_name,
                                     self.config)

    def run(self):
        """Gets file metadata from Metax.
        :returns: None
        """

        # Init workflow status database
        database = siptools_research.utils.database.Database(self.config)

        # Redirect stdout to logfile
        digiprov_log = os.path.join(self.workspace,
                                    'logs',
                                    'task-create-provenance-information.log')
        with open(digiprov_log, 'w') as log:
            with redirect_stdout(log):

                try:
                    create_premis_event(self.dataset_id,
                                        self.sip_creation_path,
                                        self.config)

                    task_result = 'success'
                    task_messages = "Provenance metadata created."
                except KeyError as exc:
                    task_result = 'failure'
                    task_messages = 'Could not create procenance metada, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message
                    database.set_status(self.document_id, 'rejected')

                finally:
                    if not 'task_result' in locals():
                        task_result = 'failure'
                        task_messages = "Creation of provenance metadata "\
                                        "failed due to unknown error."
                    database.add_event(self.document_id,
                                       self.task_name,
                                       task_result,
                                       task_messages)


def create_premis_event(dataset_id, workspace, config):
    """Gets metada from Metax and calls siptools premis_event script."""

    metadata = Metax(config).get_data('datasets', dataset_id)

    for provenance in metadata["research_dataset"]["provenance"]:
        event_type = provenance["type"]["pref_label"]["en"]
        event_datetime = provenance["temporal"]["start_date"]
        event_detail = provenance["description"]["en"]
        premis_event.main([
            event_type, event_datetime,
            "--event_detail", event_detail,
            "--event_outcome", 'success', # TODO: Hardcoded value
            "--workspace", workspace
        ])
