"""Luigi task that removes workspaces of finished workflows.
"""

import os
import shutil
from siptools_research.utils.database import Database
from siptools_research.workflowtask import WorkflowTask
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
        """Requires that preservation status has been reported"""
        return ReportPreservationStatus(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)

    def run(self):
        """Removes a finished workspace.

        :returns: None
        """
        shutil.rmtree(self.workspace)

    def complete(self):
        """Task is complete when workspace does not exist, but
        ReportPreservationStatus has finished according to workflow database.

        :returns: True or False
        """

        # Check if ReportPreservationStatus has finished
        database = Database(self.config)
        try:
            result = database.get_event_result(self.document_id,
                                               'ReportPreservationStatus')

        # TODO: Maybe these exceptions should be handled by Database module?
        except KeyError:
            # ReportPreservationStatus has not run yet
            return False

        except TypeError:
            # Workflow is not found in database
            return False

        if result != 'success':
            return False

        # Check if workspace exists
        return not os.path.exists(self.workspace)
