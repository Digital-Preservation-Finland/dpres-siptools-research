"""Luigi task that creates digital provenance information."""

import os
import luigi
from siptools.scripts import premis_event
from metax_access import Metax
from siptools_research.utils.locale import (
    get_dataset_languages, get_localized_value
)
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration


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
    config_object = Configuration(config)
    metadata = Metax(config_object.get('metax_url'),
                     config_object.get('metax_user'),
                     config_object.get('metax_password')).get_dataset(dataset_id)

    dataset_languages = get_dataset_languages(metadata)

    try:
        provenance = metadata["research_dataset"]["provenance"]
    except KeyError:
        provenance = None

    if provenance is None:
        event_type = "creation"
        event_datetime = "OPEN"
        event_detail = "Created by packaging service"
        event_outcome = "(:unav)"
        event_outcome_detail = "Value unavailable, possibly unknown"
        premis_event.main([
            event_type, event_datetime,
            "--event_detail", event_detail,
            "--event_outcome", event_outcome,
            "--event_outcome_detail", event_outcome_detail,
            "--workspace", workspace
        ])
    else:
        for provenance in metadata["research_dataset"]["provenance"]:
            event_type = get_localized_value(
                provenance["preservation_event"]["pref_label"],
                languages=dataset_languages)
            event_datetime = provenance["temporal"]["start_date"]
            event_detail = get_localized_value(
                provenance["description"],
                languages=dataset_languages)
            # Read event_outcome if it defined for this dataset
            try:
                event_outcome = get_localized_value(
                    provenance["event_outcome"]["pref_label"],
                    languages=dataset_languages)
            except KeyError:
                event_outcome = "(:unav)"
            # Read event_outcome_detail if it defined for this dataset
            try:
                event_outcome_detail = get_localized_value(
                    provenance["outcome_description"],
                    languages=dataset_languages)
            except KeyError:
                event_outcome_detail = "Value unavailable, possibly unknown"
            premis_event.main([
                event_type, event_datetime,
                "--event_detail", event_detail,
                "--event_outcome", event_outcome,
                "--event_outcome_detail", event_outcome_detail,
                "--workspace", workspace
            ])
