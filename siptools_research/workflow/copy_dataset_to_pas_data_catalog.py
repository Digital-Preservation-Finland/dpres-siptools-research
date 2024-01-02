"""Task that copies dataset metadata to PAS data catalog."""

from luigi import LocalTarget
from metax_access import DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.workflow.validate_files import ValidateFiles


class CopyToPasDataCatalog(WorkflowTask):
    """Task that copies dataset metadata to PAS data catalog.

    This task sets preservation status of dataset to
    DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION. If the dataset is in IDA
    data catalog, Metax copies the dataset to PAS data catalog. The
    preservation state of the original dataset will be set to
    DS_STATE_INITIALIZED. If the dataset already is in PAS data catalog,
    this task only sets the preservation state.

    A false target `copy-to-pas-data-catalog.finished` is
    created into workspace directory to notify luigi (and dependent
    tasks) that this task has finished.

    The task requires metadata and files to be validated.
    """

    success_message = "Dataset copied to PAS data catalog"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: ValidateMetadata task
        """
        return [ValidateMetadata(dataset_id=self.dataset_id,
                                 config=self.config),
                ValidateFiles(dataset_id=self.dataset_id,
                              config=self.config)]

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/preservation/`
                  `copy-dataset-to-pas-data-catalog.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(str(self.dataset.preservation_workspace
                               / 'copy-dataset-to-pas-data-catalog.finished'))

    def run(self):
        """Copy dataset metadata to PAS data catalog.

        :returns: ``None``
        """
        if self.dataset.preservation_state \
                < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION:
            # TODO: The preservation description will be shown to user
            # in management interface, so potentially confusing messages
            # should be avoided. Note that DS_STATE_IN_PACKAGING_SERVICE
            # would be more logical at this point, but Metax does not
            # allow setting the preservation state higher than 80.
            self.dataset.set_preservation_state(
                DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                'Packaging dataset'
            )

        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
