"""Luigi task that creates workspace directory."""

import luigi
from siptools_research.utils import utils
from siptools_research.workflowtask import WorkflowTask


class CreateWorkspace(WorkflowTask):
    """Creates directories for running the workflow.

    :returns: list of local targets
    :rtype: LocalTarget
    """

    success_message = 'Workspace directory create.'
    failure_message = 'Creating workspace directory failed'

    def output(self):
        """Creates directories for running the workflow.

        :returns: list of local targets
        :rtype: LocalTarget
        """
        return [luigi.LocalTarget(self.workspace),
                luigi.LocalTarget(self.sip_creation_path)]

    def run(self):
        """Creates workspace directory and adds event information to mongodb.

        :returns: None
        """
        utils.makedirs_exist_ok(self.workspace)
        utils.makedirs_exist_ok(self.sip_creation_path)
