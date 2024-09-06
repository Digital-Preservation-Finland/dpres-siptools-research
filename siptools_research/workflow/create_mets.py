"""Luigi task that creates METS document."""
import luigi.format
from luigi import LocalTarget
from mets_builder import (METS, MetsProfile, StructuralMap, AgentType,
                          AgentRole, StructuralMapDiv)
from mets_builder.metadata import (DigitalProvenanceEventMetadata,
                                   ImportedMetadata)
from siptools_ng.file import File
import siptools_ng.sip

from siptools_research.exceptions import (InvalidDatasetMetadataError,
                                          InvalidFileMetadataError)
from siptools_research.metax import get_metax_client
from siptools_research.utils.locale import (get_dataset_languages,
                                            get_localized_value)
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.copy_dataset_to_pas_data_catalog\
    import CopyToPasDataCatalog
from siptools_research.workflow.get_files import GetFiles

# Map event_outcome URI to a valid event outcome
EVENT_OUTCOME = {
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/success":
    "success",
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/failure":
    "failure",
    "http://uri.suomi.fi/codelist/fairdata/event_outcome/code/unknown":
    "(:unkn)",
}


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
        # while METS document is build. The plan is to move metadata
        # download to separate Task, which is required by CreateMets
        # task.
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
        # Get dataset metadata from Metax
        metax_client = get_metax_client(self.config)
        metadata = metax_client.get_dataset(self.dataset_id)
        contract_identifier = metadata.get('preservation', {})["contract"]
        contract_metadata = metax_client.get_contract(contract_identifier)
        files_metadata = metax_client.get_dataset_files(self.dataset_id)
        datacite = metax_client.get_datacite(self.dataset_id)

        # Create METS
        mets = METS(
            mets_profile=MetsProfile.RESEARCH_DATA,
            contract_id=contract_identifier,
            package_id=self.dataset.sip_identifier,
            content_id=self.dataset.sip_identifier,
            creator_name="Packaging Service",
            creator_type='OTHER',
            creator_other_type="SOFTWARE"
        )

        # Add user organization as agent
        mets.add_agent(
            name=contract_metadata["organization"]["name"],
            agent_role=AgentRole.ARCHIVIST,
            agent_type=AgentType.ORGANIZATION
        )

        # Create File objects
        files = self._create_files(files_metadata)

        # Create SIP. The physical structure map is automatically
        # created.
        sip = siptools_ng.sip.SIP.from_files(mets=mets, files=files)

        # Create logical structural map. If the structural map does not
        # contain any files, it is not added to METS
        logical_structural_map = self._create_logical_structmap(files)
        if logical_structural_map.root_div.divs:
            mets.add_structural_maps([logical_structural_map])

        # Add provenance metadata to structural maps
        provenance_metadatas = self._create_provenance_metadata(metadata)
        for provenance_metadata in provenance_metadatas:
            sip.add_metadata([provenance_metadata])
            logical_structural_map.root_div.add_metadata([provenance_metadata])

        # Add descriptive metadata to structural maps
        descriptive_metadata = ImportedMetadata(
            data_string=datacite,
            metadata_type="descriptive",
            metadata_format="OTHER",
            other_format="DATACITE",
            # TODO: Version 4.1 is chosen because
            # old siptools is using it. Is it
            # correct?
            format_version="4.1"
        )
        sip.add_metadata([descriptive_metadata])
        logical_structural_map.root_div.add_metadata([descriptive_metadata])

        # Write METS to file
        mets.generate_file_references()
        mets.write(self.output().path)

    def _create_files(self, files_metadata):
        sip_files = []
        for file_ in files_metadata:
            filepath = file_['pathname'].strip('/')
            source_filepath = (self.dataset.metadata_generation_workspace
                               / "dataset_files" / filepath)
            sip_filepath = "dataset_files/" + filepath

            # Check for conflicts between file_characteristics and
            # file_characteristics_extension
            # TODO: This should not be necessary when TPASPKT-1105 has
            # been resolved
            fc = file_["characteristics"]
            fc_extension = file_["characteristics_extension"]
            for value1, value2 in [
                    (
                        fc['file_format_version']["file_format"],
                        fc_extension["streams"]['0']["mimetype"]
                    ),
                    (
                        fc.get("encoding"),
                        fc_extension["streams"]['0'].get("charset")
                    ),
                    (
                        fc['file_format_version'].get("format_version"),
                        fc_extension["version"]
                    ),
                    (
                        fc.get("csv_delimiter"),
                        fc_extension["streams"]['0'].get("delimiter")
                    ),
                    (
                        fc.get("csv_record_separator"),
                        fc_extension["streams"]['0'].get("separator")
                    ),
                    (
                        fc.get("csv_quoting_char"),
                        fc_extension["streams"]['0'].get("quotechar")
                    ),
            ]:
                if value1 and value1 != value2:
                    raise InvalidFileMetadataError(
                        "File characteristics have changed after"
                        " metadata generation"
                    )

            # Transform string keys to integer keys in "streams"
            # dictionary
            streams = file_["characteristics_extension"]["streams"]
            streams = {
                int(key): value for key, value in streams.items()
            }
            file_["characteristics_extension"]["streams"] = streams

            sip_file = File(
                path=source_filepath,
                digital_object_path=sip_filepath,
            )
            checksum_algo_conversion = {
                'sha256': 'SHA-256',
                'sha512': 'SHA-512',
                'md5': 'MD5'
            }
            checksum_value = file_["checksum"].split(':')[-1]
            sip_file.generate_technical_metadata(
                csv_has_header=fc.get("csv_has_header"),
                file_format=fc['file_format_version']["file_format"],
                file_created_date=fc.get("file_created"),
                checksum_algorithm=checksum_algo_conversion[
                    file_["checksum"].split(':')[0]
                    ],
                checksum=checksum_value,
                scraper_result=file_["characteristics_extension"]
            )
            sip_files.append(sip_file)

        return sip_files

    def _create_logical_structmap(self, files):
        digital_objects = [file.digital_object for file in files]
        divs = []
        for category, filepaths in self._find_file_categories().items():
            paths = ["dataset_files/" + path.strip("/") for path in filepaths]
            category_digital_objects = [
                digital_object
                for digital_object
                in digital_objects
                if digital_object.path in paths
            ]
            divs.append(
                StructuralMapDiv(div_type=category,
                                 digital_objects=category_digital_objects)
            )

        root_div = StructuralMapDiv(div_type="logical")
        root_div.add_divs(divs)

        return StructuralMap(root_div=root_div,
                             structural_map_type='Fairdata-logical')

    def _create_provenance_metadata(self, metadata):
        provenance_metadatas = []
        dataset_languages = get_dataset_languages(metadata)
        provenances = metadata.get("provenance", [])
        for provenance in provenances:
            # Although it shouldn't happen, theoretically both
            # 'preservation_event' and 'lifecycle_event' could exist in
            # the same provenance metadata. 'preservation_event' is used
            # as the overriding value if both exist.
            if "preservation_event" in provenance:
                event_type = get_localized_value(
                    provenance["preservation_event"]["pref_label"],
                    languages=dataset_languages
                )
            elif "lifecycle_event" in provenance:
                event_type = get_localized_value(
                    provenance["lifecycle_event"]["pref_label"],
                    languages=dataset_languages
                )
            else:
                # TODO: Invalid metadata should be found in metadata
                # validation. So it should be unnecessary to raise
                # InvalidDatasetMetadataError here!
                raise InvalidDatasetMetadataError(
                    "Provenance metadata does not have key "
                    "'preservation_event' or 'lifecycle_event'. "
                    f"Invalid provenance: {provenance}"
                )

            try:
                event_datetime = provenance["temporal"]["start_date"]
            except KeyError:
                event_datetime = 'OPEN'

            # Add provenance title and description to eventDetail
            # element text, if present. If both are present, format as
            # "title: description". Our JSON schema validates that at
            # least one is present.
            event_detail_items = []
            if "title" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["title"],
                        languages=dataset_languages
                    )
                )
            if "description" in provenance:
                event_detail_items.append(
                    get_localized_value(
                        provenance["description"],
                        languages=dataset_languages
                    )
                )
            event_detail = ": ".join(event_detail_items)

            if "event_outcome" in provenance:
                uri = provenance["event_outcome"]["url"]
                event_outcome = EVENT_OUTCOME[uri.lower()]
            else:
                event_outcome = "(:unav)"

            event_outcome_detail = provenance.get("outcome_description", None)
            if event_outcome_detail is not None:
                event_outcome_detail = get_localized_value(
                    provenance["outcome_description"],
                    languages=dataset_languages
                )

            provenance_metadatas.append(
                DigitalProvenanceEventMetadata(
                    event_type=event_type,
                    datetime=event_datetime,
                    detail=event_detail,
                    outcome=event_outcome,
                    outcome_detail=event_outcome_detail
                )
            )

        return provenance_metadatas

    # TODO: This method might be unnecessary: TPASPKT-1107
    def _find_file_categories(self):
        """Create logical structure map of dataset files.

        Returns dictionary with filecategories as keys and filepaths as
        values.

        :returns: logical structure map dictionary
        """
        metax_client = get_metax_client(self.config)
        dataset_files = metax_client.get_dataset_files(self.dataset_id)
        dataset_metadata = metax_client.get_dataset(self.dataset_id)
        logical_struct = {}

        for dataset_file in dataset_files:

            # Get the use category of file. The path to the file in
            # logical structmap is stored in 'use_category' in metax.
            filecategory = find_file_use_category(
                dataset_file, dataset_metadata)
            if not filecategory:
                continue

            # Append path to logical_struct[filecategory] list. Create
            # list if it does not exist already
            if filecategory not in logical_struct:
                logical_struct[filecategory] = []
            logical_struct[filecategory].append(dataset_file['pathname'])

        return logical_struct


# TODO: This function might be unnecessary: TPASPKT-1107
def find_file_use_category(file_metadata, dataset_metadata):
    """Look for file with identifier from dataset metadata.

    Returns the `use_category` of file if it is found. If file is not
    found from list, return None.

    :param identifier: file identifier
    :param dataset_metadata: dataset metadata dictionary
    :returns: `use_category` attribute of file
    """
    languages = get_dataset_languages(dataset_metadata)
    category_label = file_metadata.get(
        "dataset_metadata", {}
    ).get(
        "use_category", {}
    ).get('pref_label')
    if category_label is None:
        return None

    return get_localized_value(category_label, languages=languages)
