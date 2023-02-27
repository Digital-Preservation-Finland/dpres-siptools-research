"""Task that reports preservation status after metadata validation."""

import os
from luigi import LocalTarget
from metax_access import Metax, DS_STATE_VALID_METADATA
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration


class ReportMetadataValidationResult(WorkflowTask):
    """Task that sets preservation status when metadata is validated.

    A false target `report-metadata-validation-result.finished` is created into
    workspace directory to notify luigi (and dependent tasks) that this
    task has finished.

    The task requires metadata to be validated.
    """

    success_message = "Metadata validation result reported to Metax"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: ValidateMetadata task
        """
        return ValidateMetadata(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config)

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/report-metadata-validation-result.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace, 'report-metadata-validation-result.finished'
            )
        )

    def run(self):
        """Report preservation status to Metax.

        :returns: ``None``
        """
        # Init metax
        config = Configuration(self.config)
        metax_client = Metax(
            config.get('metax_url'),
            config.get('metax_user'),
            config.get('metax_password'),
            verify=config.getboolean('metax_ssl_verification')
        )
        metax_client.set_preservation_state(
            self.dataset_id,
            DS_STATE_VALID_METADATA,
            'Metadata validated'
        )
        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
