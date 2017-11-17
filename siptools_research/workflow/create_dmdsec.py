"""Luigi task that creates descriptive metadata."""

import os
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils import database, utils
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools.scripts import import_description


class CreateDescriptiveMetadata(WorkflowTask):
    """Workflow task that creates mets dmdSec from DataCite file.
    """

    def requires(self):
        """Return required tasks. Workspace must be created.

        :returns: CreateWorkspace task
        """
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id)

    def output(self):
        """Returns task output. Task is ready when succesful event has been
        added to worklow database.

        :returns: MongoTaskResultTarget
        """
        return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name)

    def run(self):
        """
        Creates a METS dmdSec file from existing datacite.xml file. If
        unsuccessful, writes an error message into mongoDB.

        :returns: None
        """

        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        # TODO: Getting datacite.xml from Metax is not implemented
        datacite_path = os.path.join(sip_creation_path, 'datacite.xml')
        dmdsec_log = os.path.join(self.workspace,
                                  "logs",
                                  'create-descriptive-metadata.log')
        utils.makedirs_exist_ok(os.path.join(self.workspace, "logs")) 
        open(dmdsec_log, 'a')
        try:
            with open(dmdsec_log, 'w+') as log:
                with redirect_stdout(log):
                    import_description.main([datacite_path,
                                             '--workspace', sip_creation_path])

                    task_result = 'success'
                    task_messages = "DataCite metadata wrapped into METS descriptive "\
                            "metadata section."

        finally: 
            if not 'task_result' in locals():
                task_result = 'failure'
                task_messages = "Creation of provenance metadata "\
                                "failed due to unknown error."
            database.add_event(self.document_id,
                               self.task_name,
                               task_result,
                               task_messages)
