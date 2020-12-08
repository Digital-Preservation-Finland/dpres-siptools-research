"""Luigi task that creates digital provenance information."""

import os
import shutil
try:
    from tempfile import TemporaryDirectory
except ImportError:
    # TODO: Remove this when Python 2 support can be dropped
    from siptools_research.temporarydirectory import TemporaryDirectory

import luigi
from siptools.scripts import premis_event
from metax_access import Metax

from siptools_research.utils.locale import (
    get_dataset_languages, get_localized_value
)
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration


class CreateProvenanceInformation(WorkflowTask):
    """Create provenance information.

    Creates METS documents that contain PREMIS event element for each
    provenance event. Each document contains a digiprovMD element
    identified by hash generated from the document filename. The METS
    documents are written to
    `<sip_creation_path>/<uuid>-PREMIS%3AEVENT-amd.xml`. List of
    references to PREMIS events is written to
    `<workspace>/create-provenance-information.jsonl`.

    The Task requires that workspace directory is created and dataset
    metadata is validated.
    """

    success_message = "Provenance metadata created."
    failure_message = "Could not create provenance metadata"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CreateWorkspace task and ValidateMetadata task
        """
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
        """List the output targets of the task.

        :returns:  local target: 'create-provenance-information.jsonl'
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, 'create-provenance-information.jsonl')
        )

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

            _create_premis_events(self.dataset_id,
                                  temporary_workspace,
                                  self.config)

            # Move PREMIS event files to SIP creation path when all of
            # them are succesfully created to avoid atomicity problems
            for file_ in os.listdir(temporary_workspace):
                if file_.endswith('-PREMIS%3AEVENT-amd.xml'):
                    shutil.move(os.path.join(temporary_workspace, file_),
                                self.sip_creation_path)

            # Move reference file to target path
            shutil.move(
                os.path.join(temporary_workspace,
                             'premis-event-md-references.jsonl'),
                self.output().path
            )


def _create_premis_events(dataset_id, workspace, config):
    """Create premis events from provenance metadata.

    Reads dataset provenance metadata from Metax. For each provenance
    object a METS document that contains a PREMIS event element is
    created.

    :param dataset_id: dataset identifier
    :param workspace: SIP creation directory
    :param config: path to configuration file
    :returns: ``None``
    """
    config_object = Configuration(config)
    metadata = Metax(
        config_object.get('metax_url'),
        config_object.get('metax_user'),
        config_object.get('metax_password'),
        verify=config_object.getboolean('metax_ssl_verification')
    ).get_dataset(dataset_id)

    dataset_languages = get_dataset_languages(metadata)

    provenances = metadata["research_dataset"]["provenance"]

    for provenance in provenances:

        event_type = get_localized_value(
            provenance["preservation_event"]["pref_label"],
            languages=dataset_languages
        )

        try:
            event_datetime = provenance["temporal"]["start_date"]
        except KeyError:
            event_datetime = 'OPEN'

        event_detail = get_localized_value(
            provenance["description"],
            languages=dataset_languages
        )

        event_outcome = get_localized_value(
            provenance["event_outcome"]["pref_label"],
            languages=dataset_languages
        )

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
