"""Task that reports preservation status after dataset validation."""

import os
from luigi import LocalTarget
from metax_access import DS_STATE_VALID_METADATA
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.validate_files import ValidateFiles


class ReportDatasetValidationResult(WorkflowTask):
    """Task that sets preservation status when dataset is validated.

    A false target `report-dataset-validation-result.finished` is
    created into workspace directory to notify luigi (and dependent
    tasks) that this task has finished.

    The task requires metadata and files to be validated.
    """

    success_message = "Validation result reported to Metax"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: ValidateMetadata task
        """
        return [ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config),
                ValidateFiles(workspace=self.workspace,
                              dataset_id=self.dataset_id,
                              config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/report-dataset-validation-result.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace, 'report-dataset-validation-result.finished'
            )
        )

    def run(self):
        """Report preservation status to Metax.

        :returns: ``None``
        """
        self.get_metax_client().set_preservation_state(
            self.dataset_id,
            DS_STATE_VALID_METADATA,
            'Metadata and files validated'
        )
        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
