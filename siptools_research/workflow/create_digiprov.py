"""Luigi task that creates digital provenance information."""

import os
import luigi
from siptools.scripts import premis_event
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information as PREMIS event and PREMIS agent
    files in METS digiprov wrappers.
    """
    success_message = "Provenance metadata created."
    failure_message = "Could not create provenance metadata"

    def requires(self):
        """Requires workspace directory to be created and Metax metadata to be
        validated."""
        return [
            CreateWorkspace(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            ),
            ValidateMetadata(
                workspace=self.workspace,
                dataset_id=self.dataset_id,
                config=self.config
            )
        ]

    def output(self):
        """Outputs ``logs/task-create-provenance-information.log`` file."""
        return luigi.LocalTarget(
            os.path.join(self.logs_path,
                         'task-create-provenance-information.log')
        )

    def run(self):
        """Gets file metadata from Metax. Writes digital provenance information
        to ``sip-in-progress/creation-event.xml`` file.

        :returns: None
        """

        # Redirect stdout to logfile
        with self.output().open('w') as log:
            with redirect_stdout(log):
                create_premis_event(self.dataset_id,
                                    self.sip_creation_path,
                                    self.config)


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
            "--event_outcome_detail", 'blah blah', # TODO: Hardcoded value
            "--workspace", workspace
        ])
