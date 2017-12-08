"""Luigi task that removes workspaces of finished workflows.
"""

import os
import shutil
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.report_preservation_status\
    import ReportPreservationStatus


class CleanupWorkspace(WorkflowTask):
    """Remove the workspace when it is ready for cleanup.
    Tries to run task for a limited number of times until task
    sets the status of the document to pending.
    """
    success_message = 'Workspace was cleaned'
    failure_message = 'Cleaning workspace failed'

    def requires(self):
        return ReportPreservationStatus(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)

    def run(self):
        """Removes a finished workspace.

        :returns: None
        """
        shutil.rmtree(self.workspace)

    def complete(self):
        """Task is complete when workspace does not exist.

        :returns: True if workspace does not exist, else False
        """
        return not os.path.exists(self.workspace)
