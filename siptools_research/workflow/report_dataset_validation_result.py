"""Task that reports preservation status after dataset validation."""

from luigi import LocalTarget
from metax_access import DS_STATE_METADATA_CONFIRMED

from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.validate_files import ValidateFiles

DESCRIPTION_CONFIRMED_AND_VALIDATED = 'Metadata and files validated'


class ReportDatasetValidationResult(WorkflowTask):
    """Task that sets preservation status when dataset is validated.

    A false target `report-dataset-validation-result.finished` is
    created into validation workspace directory to notify luigi that
    this task has finished.

    The task requires metadata and files to be validated.
    """

    success_message = "Validation result reported to Metax"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: ValidateMetadata task
        """
        return [
            ValidateMetadata(dataset_id=self.dataset_id, config=self.config),
            ValidateFiles(dataset_id=self.dataset_id, config=self.config)
        ]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/validation`
                  `/report-dataset-validation-result.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.validation_workspace
                / 'report-dataset-validation-result.finished')
        )

    def run(self):
        """Report preservation status to Metax.

        :returns: ``None``
        """
        # TODO: As all possible states of the dataset can not be
        # described by the states that Metax currently allows (see
        # TPASPKT-998), DS_STATE_METADATA_CONFIRMED can currently mean
        # two different states:
        #
        # a) The dataset has been proposed for preservation. The dataset
        # has then been validated, and the user can accept the dataset
        # for preservation.
        #
        # b) The same user has rights for proposing and accepting the
        # dataset for preservation. The user has checked and confirmed
        # the dataset metadata, but the dataset has not been validated.
        #
        # Here, option a) should be used. Until a better solution
        # is available, the value of 'preservation_description' field is
        # used to differentiate these states from each other.
        self.dataset.set_preservation_state(
            DS_STATE_METADATA_CONFIRMED,
            DESCRIPTION_CONFIRMED_AND_VALIDATED
        )
        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
