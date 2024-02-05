"""Luigi task that creates METS document."""
import luigi.format
from luigi import LocalTarget
from siptools.scripts import compile_mets

from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_logical_structmap \
    import CreateLogicalStructMap
from siptools_research.workflow.copy_dataset_to_pas_data_catalog\
    import CopyToPasDataCatalog


class CreateMets(WorkflowTask):
    """Task that creates the METS document.

    Merges all partial METS documents found from <sip_creation_path> to
    one METS document. The METS document is written to
    <workspace>/preservation/mets.xml.

    Task requires logical structural map to be created. Task requires
    that dataset is copied to PAS data catalog to ensure that dataset
    has "preservation_dataset_version".
    """

    success_message = "METS document compiled"
    failure_message = "Compiling METS document failed"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: Required task
        """
        return [CreateLogicalStructMap(dataset_id=self.dataset_id,
                                       config=self.config),
                CopyToPasDataCatalog(dataset_id=self.dataset_id,
                                     config=self.config)]

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/preservation/mets.xml`
        :rtype: LocalTarget
        """
        return LocalTarget(
            str(self.dataset.preservation_workspace / 'mets.xml'),
            format=luigi.format.Nop
        )

    def run(self):
        """Compile all metadata files into METS document.

        :returns: ``None``
        """
        metax_client = get_metax_client(self.config)
        metadata = metax_client.get_dataset(self.dataset_id)

        # Get contract data from Metax
        contract_id = metadata["contract"]["identifier"]
        contract_metadata = metax_client.get_contract(contract_id)
        contract_identifier = contract_metadata["contract_json"]["identifier"]
        contract_org_name \
            = contract_metadata["contract_json"]["organization"]["name"]

        # Compile METS
        mets = compile_mets.create_mets(
            workspace=str(self.dataset.sip_creation_path),
            mets_profile='tpas',
            contractid=contract_identifier,
            objid=self.dataset.sip_identifier,
            contentid=self.dataset.sip_identifier,
            organization_name=contract_org_name,
            packagingservice='Packaging Service'
        )

        with self.output().open('wb') as outputfile:
            mets.write(outputfile,
                       pretty_print=True,
                       xml_declaration=True,
                       encoding='UTF-8')
