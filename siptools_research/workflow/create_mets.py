"""Luigi task that creates METS document."""
# encoding=utf8

import os
from luigi import LocalTarget
from siptools_research.utils.metax import Metax
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools.scripts import compile_mets


class CreateMets(WorkflowTask):
    """WorkflowTask that Compiles METS document."""
    success_message = "METS document compiled"
    failure_message = "Compiling METS document failed"

    def requires(self):
        """Requires METS structure map to be created.

        :returns: CreateStructMap task
        """
        return CreateStructMap(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """Creates ``sip-in-progress/mets.xml`` file

        :returns: LocalTarget
        """
        return LocalTarget(os.path.join(self.sip_creation_path, 'mets.xml'))

    def run(self):
        """Compiles all metadata files into METS document.

        :returns: None
        """

        # Redirect stdout to logfile
        mets_log = os.path.join(self.workspace, "logs", 'create-mets.log')
        with open(mets_log, 'w+') as log:
            with redirect_stdout(log):
                # Get contract id from Metax
                metadata = Metax(self.config).get_data('datasets',
                                                       self.dataset_id)
                contract_id = metadata["contract"]["id"]
                if contract_id is None:
                    raise ValueError('Dataset does not have contract id')
                if isinstance(contract_id, (int, long)):
                    contract_id = str(contract_id)

                # Compile METS
                compile_mets.main(['--workspace', self.sip_creation_path,
                                   'tpas', 'tpas', '--clean',
                                   '--contract_id', contract_id])
