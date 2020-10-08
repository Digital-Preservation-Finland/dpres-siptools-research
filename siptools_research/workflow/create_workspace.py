"""Luigi task that creates workspace directory."""

import os
import luigi
from siptools_research.workflowtask import WorkflowTask


class CreateWorkspace(WorkflowTask):
    """Creates directories required by the workflow.

    :returns: list of local targets
    :rtype: LocalTarget
    """

    success_message = 'Workspace directory created.'
    failure_message = 'Creating workspace directory failed'

    def output(self):
        """List the output targets of this Task.

        :returns: list of local targets
        """
        return [luigi.LocalTarget(self.workspace),
                luigi.LocalTarget(self.sip_creation_path)]

    def run(self):
        """Create workspace directory.

        :returns: ``None``
        """
        if not os.path.exists(self.workspace):
            os.makedirs(self.workspace)

        os.makedirs(self.sip_creation_path)
