"""Luigi task that creates METS document."""
import luigi.format
from luigi import LocalTarget
from siptools.scripts import compile_mets

from siptools_research.metax import get_metax_client
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.create_structmap import CreateStructMap
from siptools_research.workflow.create_logical_structmap \
    import CreateLogicalStructMap
from siptools_research.workflow.copy_dataset_to_pas_data_catalog\
    import CopyToPasDataCatalog
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata
from siptools_research.workflow.get_files import GetFiles


class CreateMets(WorkflowTask):
    """Creates the METS document.

    Requires that dataset is copied to PAS data catalog, as the DOI of
    the PAS version of the dataset will be included in METS.

    Writes mets.xml to preservation workspace.
    """

    success_message = "METS document created"
    failure_message = "Creating METS document failed"

    def requires(self):
        # TODO: Currently this task will download metadata from Metax
        # while METS document is build using old siptools. The plan is
        # to move metadata download to separate Task, which is required
        # by CreateMets task.
        return [
            GetFiles(
                dataset_id=self.dataset_id, config=self.config
            ),
            CopyToPasDataCatalog(
                dataset_id=self.dataset_id, config=self.config
            ),
        ]

    def output(self):
        return LocalTarget(
            str(self.dataset.preservation_workspace / 'mets.xml'),
            format=luigi.format.Nop
        )

    def run(self):
        # TODO: Rewrite this whole function using siptools-ng/mets-builder.

        # Use the run-functions of old workflow tasks to create partial
        # METS documents which are required by compile_mets-script of
        # old siptools. This is a temporary hack that will be changed in
        # next commit.
        CreateProvenanceInformation(dataset_id=self.dataset_id,
                                    config=self.config).run()
        CreateDescriptiveMetadata(dataset_id=self.dataset_id,
                                  config=self.config).run()
        CreateTechnicalMetadata(dataset_id=self.dataset_id,
                                config=self.config).run()
        CreateStructMap(dataset_id=self.dataset_id, config=self.config).run()
        CreateLogicalStructMap(dataset_id=self.dataset_id,
                               config=self.config).run()

        # Get dataset metadata from Metax
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
