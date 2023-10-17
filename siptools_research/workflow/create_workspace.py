"""Luigi task that creates workspace directory."""

from pathlib import Path
import luigi
from siptools_research.workflowtask import WorkflowTask


class CreateWorkspace(WorkflowTask):
    """Creates directories required by the workflow."""

    success_message = 'Workspace directory created.'
    failure_message = 'Creating workspace directory failed'

    def output(self):
        """List the output targets of this Task.

        :returns: `<workspace>`
        :rtype: list of local targets
        """
        return luigi.LocalTarget(self.workspace)

    def run(self):
        """Create workspace directory.

        :returns: ``None``
        """
        tmp_workspace = Path(self.output().path)
        tmp_workspace.mkdir()

        tmp_metadata_generation_workspace = (
            tmp_workspace
            / self.metadata_generation_workspace.relative_to(self.workspace)
        )
        tmp_metadata_generation_workspace.mkdir()

        tmp_validation_workspace = (
            tmp_workspace
            / self.validation_workspace.relative_to(self.workspace)
        )
        tmp_validation_workspace.mkdir()

        tmp_preservation_workspace = (
            tmp_workspace
            / self.preservation_workspace.relative_to(self.workspace)
        )
        tmp_preservation_workspace.mkdir()

        tmp_sip_creation_path = (
            tmp_workspace
            / self.sip_creation_path.relative_to(self.workspace)
        )
        tmp_sip_creation_path.mkdir()
