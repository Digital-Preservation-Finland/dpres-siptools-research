"""A wrapper task that starts/restarts all incomplete workflows."""
import luigi

from siptools_research.dataset import find_datasets, Target

from siptools_research.workflow.cleanup import Cleanup
from siptools_research.workflow.generate_metadata import GenerateMetadata
from siptools_research.workflow.report_dataset_validation_result\
    import ReportDatasetValidationResult

TARGET_TASKS = {
    Target.PRESERVATION: Cleanup,
    Target.METADATA_GENERATION: GenerateMetadata,
    Target.VALIDATION: ReportDatasetValidationResult
}


class InitWorkflows(luigi.WrapperTask):
    """A wrapper task that starts/restarts all incomplete workflows."""

    config = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: List of Tasks
        """
        for dataset in find_datasets(enabled=True, config=self.config):
            yield TARGET_TASKS[dataset.target](
                dataset_id=dataset.identifier,
                is_target_task=True,
                config=self.config
            )
