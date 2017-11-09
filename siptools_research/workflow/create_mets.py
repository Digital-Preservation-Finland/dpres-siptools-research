"""Workflow task that creates METS document"""
# encoding=utf8

import os
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.utils import  utils
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils import database
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_structmap import CreateStructMap

from siptools.scripts import compile_mets


class CreateMets(WorkflowTask):
    """Compile METS document
    """

    def requires(self):
        """Requires METS structMap and METS fileSec"""
        return {"Create StructMap":
                CreateStructMap(workspace=self.workspace)}

    def output(self):
        """Outputs a task file"""
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):
        """Compiles all metadata files into METS document.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package.

        :returns: None

        """
        utils.makedirs_exist_ok(self.sip_creation_path)
        mets_log = os.path.join(self.workspace,
                                "logs", 'create-mets.log')

        # Redirect stdout to logfile
        with open(mets_log, 'w+') as log:
            with redirect_stdout(log):
                try:
                    # Get contract id from Metax
                    metadata = Metax().get_data('datasets', self.dataset_id)
                    contract_id = metadata["contract"]["id"]
                    if contract_id is None:
                        task_result = 'failure'
                        task_messages = 'No contract id'
                        raise ValueError('Dataset does not have contract id')
                    if isinstance(contract_id, (int, long)):
                        contract_id = str(contract_id)

                    # Compile METS
                    compile_mets.main(['--workspace', self.sip_creation_path,
                                       'tpas', 'tpas', '--clean',
                                       '--contract_id', contract_id])
                    task_result = 'success'
                    task_messages = "Mets dodument created."

                except KeyError as ex:
                    task_result = 'failure'
                    task_messages = 'Could not compile mets: %s ' % ex.message

                finally:
                    if not 'task_result' in locals():
                        task_result = 'failure'
                        task_messages = "Compilation of mets document "\
                                        "failed due to unknown error."
                    database.add_event(self.document_id,
                                       self.task_name,
                                       task_result,
                                       task_messages)
