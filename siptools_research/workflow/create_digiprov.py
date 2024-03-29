"""Luigi task that creates digital provenance information."""

import os
import shutil

from tempfile import TemporaryDirectory

import luigi
from siptools.scripts import premis_event

from siptools_research.utils.locale import (
    get_dataset_languages, get_localized_value
)
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.metax import get_metax_client


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information.

    Creates METS documents that contain PREMIS event element for each
    provenance event. Each document contains a digiprovMD element
    identified by hash generated from the document filename. The METS
    documents are written to
    `<sip_creation_path>/<uuid>-PREMIS%3AEVENT-amd.xml`. List of
    references to PREMIS events is written to
    `<workspace>/preservation/create-provenance-information.jsonl`.

    The Task requires that dataset metadata is validated.
    """

    success_message = "Provenance metadata created."
    failure_message = "Could not create provenance metadata"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return ValidateMetadata(dataset_id=self.dataset_id, config=self.config)

    def output(self):
        """List the output targets of the task.

        :returns: `<workspace>/preservation/`
                  `create-provenance-information.jsonl`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(str(self.dataset.preservation_workspace
                                     / 'create-provenance-information.jsonl'))

    def run(self):
        """Create premis events.

        Reads dataset metadata from Metax and creates premis event XML
        files. Premis event XML files are written to SIP creation
        directory and premis event reference file is written to
        workspace directory.

        :returns: ``None``
        """
        config_object = Configuration(self.config)
        tmp = os.path.join(config_object.get('packaging_root'), 'tmp/')
        with TemporaryDirectory(prefix=tmp) as temporary_workspace:
            self._create_premis_events(temporary_workspace)

            # Move PREMIS event files to SIP creation path when all of
            # them are succesfully created to avoid atomicity problems.
            # PREMIS event reference file is moved to ouput target path
            # after other files are moved to SIP creation directory.
            with self.output().temporary_path() as target_path:
                shutil.move(
                    os.path.join(temporary_workspace,
                                 'premis-event-md-references.jsonl'),
                    target_path
                )

                for file_ in os.listdir(temporary_workspace):
                    shutil.move(os.path.join(temporary_workspace, file_),
                                self.dataset.sip_creation_path)

    def _create_premis_events(self, workspace):
        """Create premis events from provenance metadata.

        Reads dataset provenance metadata from Metax. For each
        provenance object a METS document that contains a PREMIS event
        element is created.

        :param workspace: SIP creation directory
        :returns: ``None``
        """
        metadata = get_metax_client(self.config).get_dataset(self.dataset_id)
        dataset_languages = get_dataset_languages(metadata)
        provenances = metadata["research_dataset"].get("provenance", [])

        for provenance in provenances:

            # Although it shouldn't happen, theoretically both
            # 'preservation_event' and 'lifecycle_event' could exist in
            # the same provenance metadata. 'preservation_event' is used
            # as the overriding value if both exist.
            if "preservation_event" in provenance:
                event_type = get_localized_value(
                    provenance["preservation_event"]["pref_label"],
                    languages=dataset_languages
                )
            elif "lifecycle_event" in provenance:
                event_type = get_localized_value(
                    provenance["lifecycle_event"]["pref_label"],
                    languages=dataset_languages
                )
            else:
                raise InvalidDatasetMetadataError(
                    "Provenance metadata does not have key "
                    "'preservation_event' or 'lifecycle_event'. "
                    f"Invalid provenance: {provenance}"
                )

            try:
                event_datetime = provenance["temporal"]["start_date"]
            except KeyError:
                event_datetime = 'OPEN'

            # Add provenance title and description to eventDetail
            # element text, if present. If both are present, format as
            # "title: description". Our JSON schema validates that at
            # least one is present.
            event_detail_items = []
            if "title" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["title"],
                        languages=dataset_languages
                    )
                )
            if "description" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["description"],
                        languages=dataset_languages
                    )
                )
            event_detail = ": ".join(event_detail_items)

            if "event_outcome" in provenance:
                event_outcome = get_localized_value(
                    provenance["event_outcome"]["pref_label"],
                    languages=dataset_languages
                )
            else:
                event_outcome = "unknown"

            event_outcome_detail = provenance.get("outcome_description", None)
            if event_outcome_detail is not None:
                event_outcome_detail = get_localized_value(
                    provenance["outcome_description"],
                    languages=dataset_languages
                )

            premis_event.premis_event(
                workspace=workspace, event_type=event_type,
                event_datetime=event_datetime, event_detail=event_detail,
                event_outcome=event_outcome,
                event_outcome_detail=event_outcome_detail
            )

        # Create a premis-event-md-references.jsonl file with empty list
        # of identifiers if dataset did not contain any provenance
        # metadata
        if not provenances:
            refence_file_path \
                = os.path.join(workspace, 'premis-event-md-references.jsonl')
            with open(refence_file_path, 'w') as reference_file:
                reference_file.write('{".": {"md_ids": []}}')
