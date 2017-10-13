"""Commandline interface to start the workflow.

To start the workflow for dataset 1234 (for example)::
   python init_workflow.py  --workspace-root /var/spool/siptools-research  1234
"""

import os
import uuid
import argparse
import luigi
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.utils import database

WORKSPACE_ROOT = '/var/spool/siptools-research'

class InitWorkflow(luigi.WrapperTask):
    """A wrapper task that starts workflow by requiring the last task of
    workflow.
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()

    def requires(self):
        """Only returns last task of the workflow.

        :returns: CreateWorkspace luigi task
        """

        # TODO: For testing purposes the task is NOT the last task, but the
        #       last IMPLEMENTED task
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

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
    arguments = parser.parse_args()

    # Set workspace name and path
    # TODO: Why should we create unique workspace for each workflow? If the
    #       workflow would be always exactly the same for the same dataset_id,
    #       luigi would take care that each dataset is sent only once to PAS.
    #       If the dataset_ids in Metax are unique and they do not change, we
    #       could use the dataset_id as document_id in our mongo database.
    workspace_name = "aineisto_%s-%s" % (arguments.dataset_id,
                                         str(uuid.uuid4()))
    workspace = os.path.join(arguments.workspace_root, workspace_name)

    # Add information to mongodb
    database.add_dataset(workspace_name, arguments.dataset_id)

    # Start luigi workflow
    luigi.run(['InitWorkflow', '--workspace', workspace,
               '--dataset-id', arguments.dataset_id])

if __name__ == '__main__':
    main()
