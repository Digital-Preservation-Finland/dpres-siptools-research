"""Luigi task that creates digital provenance information."""

import os

from luigi import local_target
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
    """Creates METS documents that contain PREMIS event element for each
    provenance event. Each document contains a digiprovMD element identified by
    hash generated from the document filename. The METS documents are written
    to `<sip_creation_path>/<event_type>-event.xml`. Because siptools requires
    these undeterministic filenames, a false target
    `create-provenance-information.finished` is created into workspace
    directory to notify luigi (and dependent tasks) that this task has
    finished.

    The Task requires that workspace directory is created and dataset metadata
    is validated.
    """

    def __init__(self, *args, **kwargs):
        """Calls WorkflowTask's __init__ and sets additional instance
        variables.
        """
        super(CreateProvenanceInformation, self).__init__(*args, **kwargs)
        config_object = Configuration(self.config)
        metax = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        self.dataset = metax.get_dataset(self.dataset_id)

    success_message = "Provenance metadata created."
    failure_message = "Could not create provenance metadata"

    def requires(self):
        """The Tasks that this Task depends on.

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
        """The output that this Task produces.

        A false target `create-provenance-information.finished` is created
        into workspace directory to notify luigi (and dependent tasks) that
        this task has finished.

        :returns: local target: `create-provenance-information.finished`
        :rtype: LocalTarget
        """
        return local_target.LocalTarget(
            os.path.join(self.workspace,
                         'create-provenance-information.finished')
        )

    def run(self):
        """Reads file metadata from Metax and writes digital provenance
        information to `sip-in-progress/creation-event.xml` file.

        :returns: None
        """
        _create_premis_events(self.dataset_id,
                              self.sip_creation_path,
                              self.config)

        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)


def _create_premis_events(dataset_id, workspace, config):
    """Reads dataset provenance metadata from Metax. For each provenance
    object a METS document that contains a PREMIS event element is created.

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

        event_datetime = provenance["temporal"]["start_date"]

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

        premis_event.create_premis_event_file(workspace,
                                              event_type,
                                              event_datetime,
                                              event_detail,
                                              event_outcome,
                                              event_outcome_detail)
