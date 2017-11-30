"""Commandline interface to start the workflow.

To start the workflow for dataset 1234 (for example)::
   python init_workflow.py  --workspace-root /var/spool/siptools-research  1234
"""

import os
import uuid
import argparse
import luigi
from siptools_research.luigi.task import WorkflowWrapperTask
import siptools_research.utils.database
from siptools_research.workflow.report_preservation_status import \
    ReportPreservationStatus

WORKSPACE_ROOT = '/var/spool/siptools-research'

class InitWorkflow(WorkflowWrapperTask):
    """A wrapper task that starts workflow by requiring the last task of
    workflow.
    """

    def requires(self):
        """Only returns last task of the workflow.

        :returns: CreateWorkspace luigi task
        """

        # TODO: For testing purposes the task is NOT the last task, but the
        #       last IMPLEMENTED task
        return ReportPreservationStatus(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)


def main():
    """Parse command line arguments and start the workflow. Generates unique id
    for the workspace. Workspace name is used as document id in MongoDB."""

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Send to dataset to digital'\
                                     'preservation service.')
    parser.add_argument('dataset_id', help="Metax dataset identifier")
    parser.add_argument('--workspace_root', default=WORKSPACE_ROOT,
                        help="Path to directory where new workspaces are "\
                             "created")
    parser.add_argument('--config', default='/etc/siptools_researc.conf',
                        help="Path to configuration file")
    arguments = parser.parse_args()

    # Set workspace name and path
    workspace_name = "aineisto_%s-%s" % (arguments.dataset_id,
                                         str(uuid.uuid4()))
    workspace = os.path.join(arguments.workspace_root, workspace_name)

    # Add information to mongodb
    database = siptools_research.utils.database.Database(arguments.config)
    database.add_dataset(workspace_name, arguments.dataset_id)

    # Start luigi workflow
    luigi.run(['SendSIPToDP', '--sip-path', workspace,
               '--dataset-id', arguments.dataset_id,
               '--workspace', workspace,
               '--config', arguments.config])


if __name__ == '__main__':
    main()

# TODO: There is not yet (2017-11-30) tests for this module.
