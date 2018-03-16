"""Commandline interface to start the workflow.

To start the workflow for dataset 1234 (for example)::

   python __main__.py  --config /etc/siptools_research.conf 1234
"""

import os
import uuid
import argparse
import luigi
from siptools_research.workflowtask import WorkflowWrapperTask
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


def preserve_dataset(dataset_id, config='/etc/siptools_research.conf'):
    """Generates unique id for the workspace and initates packaging workflow.
    Workspace name is used as document id in MongoDB. This function can be
    imported to other python modules.

    :returns: None
    """
    # Read configuration file
    conf = Configuration(config)
    workspace_root = conf.get('workspace_root')

    # Set workspace name and path
    workspace_name = "aineisto_%s-%s" % (dataset_id,
                                         str(uuid.uuid4()))
    workspace = os.path.join(workspace_root, workspace_name)

    # Add information to mongodb
    database = siptools_research.utils.database.Database(config)
    database.add_dataset(workspace_name, dataset_id)

    # Start luigi workflow
    luigi.run(['InitWorkflow',
               '--dataset-id', dataset_id,
               '--workspace', workspace,
               '--config', config])


def main():
    """Parse command line arguments and start the workflow.

    :returns: None
    """

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Send to dataset to digital'\
                                     'preservation service.')
    parser.add_argument('dataset_id', help="Metax dataset identifier")
    parser.add_argument('--config', default='/etc/siptools_research.conf',
                        help="Path to configuration file")
    args = parser.parse_args()

    preserve_dataset(args.dataset_id, args.config)


if __name__ == '__main__':
    main()

# TODO: There is not yet (2017-11-30) tests for this module.
