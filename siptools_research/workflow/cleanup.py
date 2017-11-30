"""Remove finished SIP workspaces.
This task will remove workspace if the `<workspace>/ready-for-cleanup`
file exists.
"""

import os
import shutil
import datetime

from luigi import Parameter

from siptools_research.luigi.target import MongoDBTarget

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.set_status import SetSIPStatus


class CleanupWorkspace(WorkflowTask):
    """Remove the workspace when it is ready for cleanup.
    Tries to run task for a limited number of times until task
    sets the status of the document to pending.

    :workspace: Path to workspace
    """
    workspace = Parameter()
    home_path = Parameter()

    def requires(self):
        """Require that workspace is ready for cleanup after SIP status
        has been set in previous task."""
        return {"Set SIP status": SetSIPStatus(workspace=self.workspace,
                                               home_path=self.home_path,
                                               config=self.config)}

    def run(self):
        """Remove a finished workspace.

        Note: Sometimes GlusterFS fails to remove directory if deleted
        with rmtree() at once. Here we try to delete workspace
        incrementally, deleting transfers, logs and task-output-files
        directories as last ones. These three are ones needed to
        recognize finished workspace. Writes outcome to mongoDB.

        :returns: None
        """
        paths = [
            'sip-in-progress',
            'created-sip',
            'reports',
            'transfers',
            'logs',
            'task-output-files']
        paths = [os.path.join(self.workspace, path) for path in paths]

        document_id = os.path.basename(self.workspace)
        mongo_task = MongoDBTarget(document_id, 'wf_tasks.cleanup-workspace')
        mongo_status = MongoDBTarget(document_id, 'status')

        for path in paths:
            if os.path.isdir(path):
                shutil.rmtree(path)
        shutil.rmtree(self.workspace)

        if not os.path.exists(self.workspace):
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'success',
                'messages': ("Workspace cleaned up.")
            }
            mongo_task.write(task_result)

        elif os.path.exists(self.workspace):
            count = 0
            task_result = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'result': 'failure',
                'messages': ("Error in cleaning up workspace.")
            }
            mongo_task.write(task_result)
            cleanup_log = os.path.join(self.workspace, 'logs',
                                       'task-cleanup')

            # Check how many times task has been run
            if os.path.isfile(cleanup_log):
                log_file = open(cleanup_log, 'r')
                first_line = log_file.readline()
                count = int(first_line.split(' ', 1)[0])
                log_file.close()

            # Set status to pending if task has been run < 10 times
            if count < 10:
                count += 1
                with open(cleanup_log, 'w') as outfile:
                    outfile.write(count)
            else:
                mongo_status.write('pending')

    def complete(self):
        """Task is complete when workspace does not exist.

        :returns: True if workspace does not exist, else False
        """
        return not os.path.exists(self.workspace)
