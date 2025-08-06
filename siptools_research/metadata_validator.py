"""Dataset metadata validation tools."""

from __future__ import annotations
import copy
import os
import jsonschema

import siptools_research.schemas
from file_scraper.defaults import (
    BIT_LEVEL_WITH_RECOMMENDED,
    RECOMMENDED,
    UNACCEPTABLE,
)

from siptools_research.exceptions import (
    InvalidDatasetFileError,
    InvalidDatasetMetadataError,
    InvalidFileMetadataError,
    file_error_collector,
)

from siptools_research.metax import get_metax_client


def validate_metadata(
    dataset_id: str,
    config: str | os.PathLike = "/etc/siptools_research.conf",
    dummy_doi: bool = False,
):
    """
    Helper function for using the MetadataValidator object. The function
    creates a MetadataValidator object and validates data with it.

    :param dataset_id: Dataset identifier
    :param config: Configuration file path
    :param dummy_doi: `True`, if dummy preservation identifier is to be
                      used
    :return: `True`, if dataset metadata is valid.
    """
    validator_obj = MetadataValidator(dataset_id, config, dummy_doi)
    validation_result = validator_obj.validate()
    return validation_result


class MetadataValidator:
    """
    A class for metadata validation. Validates data with the `validate` method.
    The class can also be used with the `validate_metadata` helper function
    above.
    """

    def __init__(
        self,
        dataset_id: str,
        config: str | os.PathLike = "/etc/siptoos_research.conf",
        dummy_doi: bool = False,
    ):
        """
        Construction method.

        :param dataset_id: Dataset identifier
        :param config: Configuration file path
        :param dummy_doi: `True`, if dummy preservation identifier is to be
                          used
        """

        self.dataset_id = dataset_id
        self.config = config
        self.dummy_doi = dummy_doi

    def validate(self):
        """
        Validate dataset.

        Reads dataset metadata and file metadata from Metax and validates
        them against schemas. Raises `InvalidDatasetFileError` or
        `InvalidDatasetMetadataError` if dataset is invalid.

        :return: `True`, if dataset metadata is valid.
        """
        # Get dataset metadata from Metax
        self.metax_client = get_metax_client(self.config)
        self.dataset_metadata = self.metax_client.get_dataset(self.dataset_id)

        # Validate dataset metadata
        self._validate_dataset_metadata()

        # Validate file metadata for each file in dataset files
        self._validate_file_metadata()

        return True

    def _validate_dataset_metadata(self):
        """
        Validate dataset metadata against the schema. Raises
        `InvalidDatasetMetadataError` if dataset metadata is invalid.
        """
        schema = copy.deepcopy(
            siptools_research.schemas.DATASET_METADATA_SCHEMA
        )
        # If `dummy_doi` is used, drop preservation_identifier from schema
        if self.dummy_doi:
            schema["properties"]["preservation"]["required"] = ["contract"]
            del schema["properties"]["preservation"]["properties"]["id"]

        try:
            jsonschema.validate(self.dataset_metadata, schema)
        except jsonschema.ValidationError as exc:
            raise InvalidDatasetMetadataError(str(exc)) from exc

    def _validate_file_metadata(self):
        """
        Validate file metadata. Raises `InvalidDatasetMetadataError` if the
        dataset do not contain any files.

        This function uses following functions for validation:
            * `_validate_file_dpres_links`
            * `_check_file_format_version`
            * `_check_draft7validator`
            * `_check_file_path`
            * `_check_grade`
        """
        self.dataset_files = self.metax_client.get_dataset_files(
            self.dataset_metadata["id"]
        )
        if not self.dataset_files:
            raise InvalidDatasetMetadataError(
                "Dataset must contain at least one file"
            )

        # Validate that every DPRES compatible and bit-level file pair
        # is included in the dataset
        self._validate_file_dpres_links()

        with file_error_collector() as self.collect_error:
            for self.file_metadata in self.dataset_files:
                # Validate metadata against JSON schema. The schema contains
                # properties introduced in JSON schema draft 7. Using
                # Draft7Validator ensures that older validators that do not
                # support draft 7 are not used, in which case part of the
                # schema would be ignored without any warning.

                self.file_path = self.file_metadata["pathname"]
                self.is_linked_bitlevel = bool(
                    self.file_metadata["pas_compatible_file"]
                )
                self.characteristics = (
                    self.file_metadata["characteristics"] or {}
                )
                self.file_identifier = self.file_metadata["id"]

                self._check_file_format_version()
                self._check_draft7validator()
                self._check_file_path()
                self._check_grade()

    def _check_file_format_version(self):
        """
        Check that non bit-level file has a file format version and collect an
        error if it doesn't have it.
        """

        file_format_version = self.characteristics["file_format_version"][
            "file_format"
        ]
        if not file_format_version and not self.is_linked_bitlevel:
            self.collect_error(
                InvalidFileMetadataError(
                    f"Non bit-level file must have `file_format_version` "
                    f"set: {self.file_path}",
                    files=[self.file_metadata],
                )
            )

    def _check_draft7validator(self):
        """
        Validate file metadata with `jsonschema.Draft7Validator`. If the check
        fails, collect the error.
        """

        try:
            jsonschema.Draft7Validator(
                siptools_research.schemas.FILE_METADATA_SCHEMA
            ).validate(self.file_metadata)
        except jsonschema.ValidationError as exc:
            self.collect_error(
                InvalidFileMetadataError(
                    f"Validation error in metadata of "
                    f"{self.file_path}: {str(exc)}",
                    files=[self.file_metadata],
                )
            )

    def _check_file_path(self):
        """
        Check that file path does not point outside SIP. If the check fails,
        collect the error.
        """
        normalised_path = os.path.normpath(self.file_path.strip("/"))
        if normalised_path.startswith(".."):
            self.collect_error(
                InvalidFileMetadataError(
                    f"The file path of file {self.file_identifier} is invalid:"
                    f" {self.file_path}",
                    files=[self.file_metadata],
                )
            )

    def _check_grade(self):
        """
        Check that files with `BIT_LEVEL_WITH_RECOMMENDED` or
        `UNACCEPTABLE` grade have a PAS compatible file. Collect the error.
        """

        char_ext = self.file_metadata["characteristics_extension"] or {}
        is_incomplete_bitlevel = (
            char_ext.get("grade", None)
            in (BIT_LEVEL_WITH_RECOMMENDED, UNACCEPTABLE)
            and not self.is_linked_bitlevel
        )

        if is_incomplete_bitlevel:
            self.collect_error(
                InvalidFileMetadataError(
                    f"File {self.file_identifier} with '{char_ext['grade']}' "
                    f"grade is not linked to a PAS compatible file",
                    files=[self.file_metadata],
                )
            )

        else:
            is_linked_pas_compatible = bool(
                self.file_metadata["non_pas_compatible_file"]
            )
            # Check that PAS compatible file has a good enough grade
            if is_linked_pas_compatible and char_ext["grade"] != RECOMMENDED:
                self.collect_error(
                    InvalidFileMetadataError(
                        f"File {self.file_identifier} with "
                        f"grade '{char_ext['grade']}' marked as PAS "
                        f"compatible does not have the required "
                        f"'{RECOMMENDED}' grade",
                        files=[self.file_metadata],
                    )
                )

    def _validate_file_dpres_links(self):
        """
        Validate that dataset files contain every DPRES & non DPRES compatible
        file pair if any exist. Raises InvalidDatasetFileError:
        If any linked files are missing their DPRES and/or bit-level
        counterparts.
        """
        dpres2non_dpres_file_id = {}
        non_dpres2dpres_file_id = {}

        # Create DPRES compatibility links
        for file in self.dataset_files:
            if id_ := file["non_pas_compatible_file"]:
                non_dpres2dpres_file_id[id_] = file["id"]

            if id_ := file["pas_compatible_file"]:
                dpres2non_dpres_file_id[id_] = file["id"]

        lone_non_dpres_ids = [
            id_
            for id_ in dpres2non_dpres_file_id.values()
            if id_ not in non_dpres2dpres_file_id
        ]

        lone_dpres_ids = [
            id_
            for id_ in non_dpres2dpres_file_id.values()
            if id_ not in dpres2non_dpres_file_id
        ]

        if lone_dpres_ids or lone_non_dpres_ids:
            error = "File linkings for DPRES compatible files are incomplete. "
            all_lone_ids = lone_non_dpres_ids + lone_dpres_ids

            if lone_dpres_ids and not lone_non_dpres_ids:
                error += (
                    "Dataset contains DPRES compatible files without "
                    "bit-level counterparts."
                )
            elif not lone_dpres_ids and lone_non_dpres_ids:
                error += (
                    "Dataset contains bit-level files without "
                    "DPRES compatible counterparts."
                )
            else:
                error += (
                    "Dataset contains both bit-level and DPRES compatible "
                    "files without DPRES compatible / bit-level counterparts."
                )

            lone_files = [
                file_
                for file_ in self.dataset_files
                if file_["id"] in all_lone_ids
            ]

            raise InvalidDatasetFileError(
                error, files=lone_files, is_dataset_error=True
            )
