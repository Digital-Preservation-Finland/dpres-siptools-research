"""Luigi task that creates digital provenance information."""
import shutil

from siptools.scripts import premis_event

from siptools_research.utils.locale import (
    get_dataset_languages, get_localized_value
)
from siptools_research.workflowtask import WorkflowTask
from siptools_research.exceptions import InvalidDatasetMetadataError
from siptools_research.metax import get_metax_client


class CreateProvenanceInformation(WorkflowTask):

    def run(self):
        """Create provenance information.

        Creates METS documents that contain PREMIS event element for
        each provenance event. Each document contains a digiprovMD
        element identified by hash generated from the document filename.
        The METS documents are written to
        `<sip_creation_path>/<uuid>-PREMIS%3AEVENT-amd.xml`. Premis
        event reference is written to
        <sip_creation_path>/premis-event-md-references.jsonl.

        The premis-event-md-references.jsonl file is copied to
        `<workspace>/preservation/create-provenance-information.jsonl`,
        because logical structuremap creation needs a reference file
        that contains only provenance events. Other events created by
        siptools-scripts would break the logical structuremap.
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
                workspace=self.dataset.sip_creation_path,
                event_type=event_type,
                event_datetime=event_datetime, event_detail=event_detail,
                event_outcome=event_outcome,
                event_outcome_detail=event_outcome_detail
            )

        # Create a copy of premis-event-md-references.jsonl before other
        # siptools-scripts will pollute it. It is needed for logical
        # structuremap creation.
        if provenances:
            shutil.copy(
                self.dataset.sip_creation_path
                / 'premis-event-md-references.jsonl',
                self.dataset.preservation_workspace
                / 'create-provenance-information.jsonl'
            )
