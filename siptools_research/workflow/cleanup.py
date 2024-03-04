"""Luigi task that removes the workspace."""
import shutil

from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.report_preservation_status \
    import ReportPreservationStatus


class Cleanup(WorkflowTask):
    """Removes the workspace.

    Requires that preservation status has been reported to Metax.

    The task is complete when workspace has been removed.
    """

    success_message = 'Workspace was cleaned'
    failure_message = 'Cleaning workspace failed'

    def requires(self):
        return ReportPreservationStatus(dataset_id=self.dataset_id,
                                        config=self.config)

    def complete(self):
        return not self.dataset.workspace_root.exists()

    def run(self):
        shutil.rmtree(self.dataset.workspace_root)
