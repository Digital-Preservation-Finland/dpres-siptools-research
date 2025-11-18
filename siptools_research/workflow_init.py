"""A wrapper task that starts/restarts all incomplete workflows."""
import luigi

from siptools_research.database import connect_mongoengine
from siptools_research.config import Configuration
from siptools_research.workflow import TARGET_TASKS, find_workflows


class InitWorkflows(luigi.WrapperTask):
    """A wrapper task that starts/restarts all incomplete workflows."""

    config = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: List of Tasks
        """
        # Connect to database here, since this is the entry point for
        # `siptools-research.service`
        configuration = Configuration(self.config)
        connect_mongoengine(
            host=configuration.get("mongodb_host"),
            port=configuration.get("mongodb_port"),
        )

        for workflow in find_workflows(enabled=True, config=self.config):
            task = TARGET_TASKS[workflow.target](
                dataset_id=workflow.dataset.identifier,
                config=self.config
            )

            # Skip completed workflows
            if task.complete():
                continue

            # Skip invalid datasets
            if workflow.dataset.errors:
                continue

            yield task
