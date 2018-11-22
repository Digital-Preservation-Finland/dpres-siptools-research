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
    """Compiles METS document. Task requires METS structure map to be created.
    """
    success_message = "METS document compiled"
    failure_message = "Compiling METS document failed"

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: CreateStructMap task
        """
        return CreateStructMap(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        """The output that this Task produces.

        A false target ``create-mets.finished`` is created into
        workspace directory to notify luigi (and dependent tasks) that this
        task has finished.

        :returns: list of local targets: ``sip-in-progress/mets.xml`` and
            create-mets.finished
        :rtype: LocalTarget
        """
        targets = []
        targets.append(LocalTarget(os.path.join(self.sip_creation_path,
                                                'mets.xml')))
        targets.append(LocalTarget(os.path.join(self.workspace,
                                                'create-mets.finished')))
        return targets

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
        contract_id = metadata["contract"]["identifier"]

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
        with self.output()[-1].open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
