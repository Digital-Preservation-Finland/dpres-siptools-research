"""Commandline interface to start the workflow.

To start the workflow for dataset 1234 (for example)::

   python init_workflow.py  --config /etc/siptools_research.conf
   --workspace-root /var/spool/siptools-research 1234
"""

import os
import uuid
import argparse
import luigi
from siptools_research.luigi.task import WorkflowWrapperTask
from siptools_research.config import Configuration
import siptools_research.utils.database
from siptools_research.workflow.report_preservation_status import \
    ReportPreservationStatus

class InitWorkflow(WorkflowWrapperTask):
    """A wrapper task that starts workflow by requiring the last task of
    workflow.
    """

    def requires(self):
        """Only returns last task of the workflow.

        :returns: ReportPreservationStatus task
        """

        # TODO: For testing purposes the task is NOT the last task, but the
        #       last IMPLEMENTED task
        return ReportPreservationStatus(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)


def main():
    """Parse command line arguments and start the workflow. Generates unique id
    for the workspace. Workspace name is used as document id in MongoDB.

    :returns: None
    """

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Send to dataset to digital'\
                                     'preservation service.')
    parser.add_argument('dataset_id', help="Metax dataset identifier")
    parser.add_argument('--workspace_root', default=None,
                        help="Path to directory where new workspaces are "\
                             "created")
    parser.add_argument('--config', default='/etc/siptools_research.conf',
                        help="Path to configuration file")
    args = parser.parse_args()

    # Read configuration file
    conf = Configuration(args.config)
    workspace_root = conf.get('workspace_root')

    # Override configuration file with commandline arguments
    if args.workspace_root:
        workspace_root = args.workspace_root

    # Set workspace name and path
    workspace_name = "aineisto_%s-%s" % (args.dataset_id,
                                         str(uuid.uuid4()))
    workspace = os.path.join(workspace_root, workspace_name)

    # Add information to mongodb
    database = siptools_research.utils.database.Database(args.config)
    database.add_dataset(workspace_name, args.dataset_id)

    # Start luigi workflow
    luigi.run(['InitWorkflow',
               '--dataset-id', args.dataset_id,
               '--workspace', workspace,
               '--config', args.config])


if __name__ == '__main__':
    main()

# TODO: There is not yet (2017-11-30) tests for this module.
