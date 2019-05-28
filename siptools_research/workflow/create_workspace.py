"""Luigi task that creates workspace directory."""

import os
import luigi
from siptools_research.workflowtask import WorkflowTask
from metax_access import Metax, DS_STATE_IN_PACKAGING_SERVICE
from siptools_research.config import Configuration


class CreateWorkspace(WorkflowTask):
    """Creates directories required by the workflow.

    :returns: list of local targets
    :rtype: LocalTarget
    """

    success_message = 'Workspace directory create.'
    failure_message = 'Creating workspace directory failed'

    def output(self):
        """The output that this Task produces.

        :returns: list of local targets
        """
        return [luigi.LocalTarget(self.workspace),
                luigi.LocalTarget(self.sip_creation_path)]

    def run(self):
        """Sets dataset's preservation state as In Packaging Service
        and creates workspace directory.

        :returns: ``None``
        """
        self.update_dataset_preservation_state()
        if not os.path.exists(self.workspace):
            os.makedirs(self.workspace)

        os.makedirs(self.sip_creation_path)

    def update_dataset_preservation_state(self):
        """Sets dataset's preservation state as In Packaging Service
        if not already set.

        :returns: ``None``
        """
        config_object = Configuration(self.config)
        metax = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification'))
        dataset = metax.get_dataset(self.dataset_id)
        # Do not change preservation_state_modified timestamp unnecessarily
        if dataset['preservation_state'] != DS_STATE_IN_PACKAGING_SERVICE:
            metax.set_preservation_state(
                self.dataset_id,
                state=DS_STATE_IN_PACKAGING_SERVICE,
                system_description='In packaging service'
            )
