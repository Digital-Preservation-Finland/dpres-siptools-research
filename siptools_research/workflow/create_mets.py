"""Luigi task that creates METS document."""
# encoding=utf8

import os
from luigi import LocalTarget
from metax_access import Metax
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
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

        # Get contract id from Metax
        config_object = Configuration(self.config)
        metax_client = Metax(config_object.get('metax_url'),
                             config_object.get('metax_user'),
                             config_object.get('metax_password'))
        metadata = metax_client.get_dataset(self.dataset_id)
        contract_id = metadata["contract"]["id"]

        contract_metadata = metax_client.get_contract(contract_id)
        contract_identifier = contract_metadata["contract_json"]["identifier"]
        contract_org_name \
            = contract_metadata["contract_json"]["organization"]["name"]

        # Compile METS
        compile_mets.main(['tpas',
                           contract_org_name.encode('utf-8'),
                           contract_identifier,
                           '--workspace', self.sip_creation_path,
                           '--clean',
                           '--objid', self.dataset_id,
                           '--packagingservice', 'Packaging Service'])
