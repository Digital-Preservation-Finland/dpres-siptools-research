"""Luigi task that creates METS document."""
from collections import defaultdict

import luigi.format
import siptools_ng.sip
from luigi import LocalTarget
from mets_builder import (METS, AgentRole, AgentType, MetsProfile,
                          StructuralMap, StructuralMapDiv)
from mets_builder.metadata import (DigitalProvenanceEventMetadata,
                                   ImportedMetadata)
from siptools_ng.file import File

from siptools_research.exceptions import (InvalidDatasetMetadataError,
                                          InvalidFileMetadataError)
from siptools_research.metax import (CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL,
                                     get_metax_client)
from siptools_research.locale import (get_dataset_languages,
                                      get_localized_value)
from siptools_research.workflow.copy_dataset_to_pas_data_catalog import \
    CopyToPasDataCatalog
from siptools_research.workflow.get_files import GetFiles
from siptools_research.workflowtask import WorkflowTask

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

        datacite = self.dataset.get_datacite()

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
        logical_structural_map \
            = self._create_logical_structmap(files, metadata, files_metadata)
        if logical_structural_map.root_div.divs:
            mets.add_structural_maps([logical_structural_map])

        # Add provenance metadata to structural maps
        provenance_metadatas = self._create_provenance_metadata(metadata)
        for provenance_metadata in provenance_metadatas:
            sip.add_metadata([provenance_metadata])
            logical_structural_map.root_div.add_metadata([provenance_metadata])

        # Add descriptive metadata to structural maps
        descriptive_metadata = ImportedMetadata.from_string(datacite)
        sip.add_metadata([descriptive_metadata])
        logical_structural_map.root_div.add_metadata([descriptive_metadata])

        # Write METS to file
        mets.generate_file_references()
        mets.write(self.output().path)

    def _create_files(self, files_metadata):
        sip_files = []

        # Track PAS compatible to non PAS compatible file linkings.
        # For any pair, both of the files have to exist on the SIP.
        dpres2non_dpres_file_id = {}
        non_dpres2dpres_file_id = {}
        id2sip_file = {}

        for file_ in files_metadata:
            filepath = file_['pathname'].strip('/')
            source_filepath = (self.dataset.metadata_generation_workspace
                               / "dataset_files" / filepath)
            sip_filepath = "dataset_files/" + filepath
            is_linked_bitlevel = bool(file_["pas_compatible_file"])

            # Check for conflicts between file_characteristics and
            # file_characteristics_extension
            # TODO: This should not be necessary when TPASPKT-1105 has
            # been resolved
            fc = file_["characteristics"]
            fc_extension = file_["characteristics_extension"]

            metadata_pairs = [
                (
                    fc["encoding"],
                    fc_extension["streams"]["0"].get("charset")
                ),
                (
                    fc["csv_delimiter"],
                    fc_extension["streams"]["0"].get("delimiter")
                ),
                (
                    CSV_RECORD_SEPARATOR_ENUM_TO_LITERAL[
                        fc["csv_record_separator"]
                    ],
                    fc_extension["streams"]["0"].get("separator")
                ),
                (
                    fc["csv_quoting_char"],
                    fc_extension["streams"]["0"].get("quotechar")
                ),
            ]

            # Do not check for file format version if the file is a bit-level
            # file with a DPRES compatible counterpart.
            # In such scenarios we'll accept pretty much anything for the
            # bit-level file, even files we cannot identify.
            if not is_linked_bitlevel:
                metadata_pairs += [
                    (
                        fc["file_format_version"]["file_format"],
                        fc_extension["streams"]["0"]["mimetype"]
                    ),
                    (
                        fc["file_format_version"]["format_version"],
                        fc_extension["version"]
                    ),
                ]

            for value1, value2 in metadata_pairs:
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
            file_format_version = fc["file_format_version"] or {}
            sip_file.generate_technical_metadata(
                csv_has_header=fc.get("csv_has_header"),
                file_format=file_format_version.get("file_format"),
                checksum_algorithm=checksum_algo_conversion[
                    file_["checksum"].split(':')[0]
                    ],
                checksum=checksum_value,
                scraper_result=file_["characteristics_extension"]
            )

            # Create DPRES compatibility links
            if id_ := file_["non_pas_compatible_file"]:
                non_dpres2dpres_file_id[id_] = file_["id"]

            if id_ := file_["pas_compatible_file"]:
                dpres2non_dpres_file_id[id_] = file_["id"]

            id2sip_file[file_["id"]] = sip_file

            sip_files.append(sip_file)

        self._link_files(
            id2sip_file=id2sip_file,
            dpres2non_dpres_file_id=dpres2non_dpres_file_id,
            non_dpres2dpres_file_id=non_dpres2dpres_file_id
        )

        return sip_files

    def _link_files(
            self,
            id2sip_file,
            dpres2non_dpres_file_id, non_dpres2dpres_file_id):
        """
        Link DPRES compatible and non-DPRES compatible (i.e. bit-level) files
        together
        """
        for dpres_id, non_dpres_id in dpres2non_dpres_file_id.items():
            bitlevel_file = id2sip_file[non_dpres_id]
            dpres_file = id2sip_file[dpres_id]

            bitlevel_file.digital_object.use = \
                "fi-dpres-no-file-format-validation"

            bitlevel_file_techmd = next(
                metadata for metadata in bitlevel_file.metadata
                if metadata.metadata_type.value == "technical"
                and metadata.metadata_format.value == "PREMIS:OBJECT"
            )
            dpres_file_techmd = next(
                metadata for metadata in dpres_file.metadata
                if metadata.metadata_type.value == "technical"
                and metadata.metadata_format.value == "PREMIS:OBJECT"
            )

            event = DigitalProvenanceEventMetadata(
                event_type="normalization",
                detail="Normalization of digital object",
                outcome="success",
                outcome_detail=(
                    "Source file format has been normalized. "
                    "Outcome object has been created as a result."
                )
            )
            event.link_object_metadata(
                bitlevel_file_techmd,
                object_role="source"
            )
            event.link_object_metadata(
                dpres_file_techmd,
                object_role="outcome"
            )

            # FIXME: mets-builder currently has broken behavior and will
            # also add the linked metadata from 'event' to both files
            # if 'File.add_metadata' is used. This linked metadata includes the
            # technical metadata from the other file.
            #
            # This method of adding metadata is undocumented but should work
            # to avoid the issue until it's fixed.
            dpres_file.digital_object.metadata.add(event)
            bitlevel_file.digital_object.metadata.add(event)

    def _create_logical_structmap(self, files, metadata, files_metadata):

        # Map filepaths to use category
        use_category_files = defaultdict(list)
        for file in files_metadata:

            use_category_object = file["dataset_metadata"]["use_category"]
            if not use_category_object:
                # Use category is not defined for this file, so the file
                # is not added to logical structure map
                continue

            use_category = get_localized_value(
                use_category_object["pref_label"],
                languages=get_dataset_languages(metadata)
            )

            use_category_files[use_category].append(file['pathname'])

        # Create structural map div for each use category
        divs = []
        digital_objects = [file.digital_object for file in files]
        for use_category, filepaths in use_category_files.items():
            paths = ["dataset_files/" + path.strip("/") for path in filepaths]
            category_digital_objects = [
                digital_object
                for digital_object
                in digital_objects
                if digital_object.path in paths
            ]
            divs.append(
                StructuralMapDiv(div_type=use_category,
                                 digital_objects=category_digital_objects)
            )

        # Create the structural map
        root_div = StructuralMapDiv(div_type="logical")
        root_div.add_divs(divs)
        return StructuralMap(root_div=root_div,
                             structural_map_type='Fairdata-logical')

    def _create_provenance_metadata(self, metadata):
        provenance_metadatas = []
        dataset_languages = get_dataset_languages(metadata)
        provenances = metadata["provenance"]
        for provenance in provenances:
            if provenance["lifecycle_event"] is None:
                # TODO: Invalid metadata should be found in metadata
                # validation. So it should be unnecessary to raise
                # InvalidDatasetMetadataError here!
                raise InvalidDatasetMetadataError(
                    "Provenance metadata does not have key "
                    "'lifecycle_event'. "
                    f"Invalid provenance: {provenance}"
                )

            event_type = get_localized_value(
                provenance["lifecycle_event"]["pref_label"],
                languages=dataset_languages
            )

            try:
                event_datetime = provenance["temporal"]["start_date"]
            except TypeError:
                event_datetime = 'OPEN'

            # eventDetail is build from provenance title and optional
            # provenance description
            event_detail = get_localized_value(
                provenance["title"],
                languages=dataset_languages
            )
            if provenance["description"] is not None:
                event_detail += ": " + get_localized_value(
                    provenance["description"],
                    languages=dataset_languages
                )

            if provenance["event_outcome"] is not None:
                uri = provenance["event_outcome"]["url"]
                event_outcome = EVENT_OUTCOME[uri.lower()]
            else:
                event_outcome = "(:unav)"

            event_outcome_detail = provenance["outcome_description"]
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
