"""Luigi task that creates METS document."""
import os

import luigi.format
from luigi import LocalTarget

from metax_access import Metax
from siptools.scripts import compile_mets

from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_logical_structmap \
    import CreateLogicalStructMap


class CreateMets(WorkflowTask):
    """Task that creates the METS document.

    Merges all partial METS documents found from <sip_creation_path> to
    one METS document. The METS document is written to
    <workspace>/mets.xml.

    Task requires logical structural map to be created.
    """

    success_message = "METS document compiled"
    failure_message = "Compiling METS document failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: Required task
        """
        return CreateLogicalStructMap(workspace=self.workspace,
                                      dataset_id=self.dataset_id,
                                      config=self.config)

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/mets.xml`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(self.workspace, 'mets.xml'),
            format=luigi.format.Nop
        )

    def run(self):
        """Compile all metadata files into METS document.

        :returns: ``None``
        """
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        metadata = metax_client.get_dataset(self.dataset_id)

        # Get preservation_identifier from Metax
        preservation_id = metadata["preservation_identifier"]

        # Get contract data from Metax
        contract_id = metadata["contract"]["identifier"]
        contract_metadata = metax_client.get_contract(contract_id)
        contract_identifier = contract_metadata["contract_json"]["identifier"]
        contract_org_name \
            = contract_metadata["contract_json"]["organization"]["name"]

        # Compile METS
        mets = compile_mets.create_mets(
            workspace=self.sip_creation_path,
            mets_profile='tpas',
            contractid=contract_identifier,
            objid=preservation_id,
            organization_name=contract_org_name,
            packagingservice='Packaging Service'
        )

        with self.output().open('wb') as outputfile:
            mets.write(outputfile,
                       pretty_print=True,
                       xml_declaration=True,
                       encoding='UTF-8')
