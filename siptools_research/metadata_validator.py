"""Dataset metadata validation tools."""

from __future__ import annotations
import os
import copy
from typing import Callable
import jsonschema

from file_scraper.defaults import (
    BIT_LEVEL_WITH_RECOMMENDED,
    RECOMMENDED,
    UNACCEPTABLE,
)
from metax_access.response import MetaxFile

import siptools_research.schemas
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
) -> None:
    """
    Helper function for using the MetadataValidator object.

    :param dataset_id: Dataset identifier
    :param config: Configuration file path
    :param dummy_doi: True if dummy preservation identifier is to be used
    """
    validator = MetadataValidator(dataset_id, config, dummy_doi)
    validator.validate()


class MetadataValidator:
    """
    Class for metadata validation. Validates data with the `validate` method.
    The class can also be used with the `validate_metadata` helper function
    above.
    """

    def __init__(
        self,
        dataset_id: str,
        config: str | os.PathLike = "/etc/siptoos_research.conf",
        dummy_doi: bool = False,
    ):
        self.dataset_id = dataset_id
        self.config = config
        self.dummy_doi = dummy_doi
        self.metax_client = get_metax_client(self.config)

    def validate(self) -> None:
        """
        Validate dataset.

        Reads dataset metadata and file metadata from Metax and validates
        them against schemas. Raises `InvalidDatasetFileError` or
        `InvalidDatasetMetadataError` if dataset is invalid.
        """
        dataset_metadata = self.metax_client.get_dataset(self.dataset_id)
        self._validate_dataset_metadata(dataset_metadata)

        dataset_files = self.metax_client.get_dataset_files(
            dataset_metadata["id"]
        )
        self._validate_file_metadata(dataset_files)

    def _validate_dataset_metadata(self, metadata: dict) -> None:
        """
        Validate dataset metadata against the schema. Raises
        `InvalidDatasetMetadataError` if dataset metadata is invalid.

        :param metadata: Dataset metadata in a dictionary
        """
        schema = copy.deepcopy(
            siptools_research.schemas.DATASET_METADATA_SCHEMA
        )
        if self.dummy_doi:
            schema["properties"]["preservation"]["required"] = ["contract"]
            schema["properties"]["preservation"]["properties"].pop("id", None)

        try:
            jsonschema.validate(metadata, schema)
        except jsonschema.ValidationError as exc:
            raise InvalidDatasetMetadataError(str(exc)) from exc

    def _validate_file_metadata(self, files: list[MetaxFile]) -> None:
        """
        Validate file metadata. Raises `InvalidDatasetMetadataError` if the
        dataset do not contain any files.

        :param files: Dataset files in a list
        """
        if not files:
            raise InvalidDatasetMetadataError(
                "Dataset must contain at least one file"
            )

        # Validate that every DPRES compatible and bit-level file pair
        # is included in the dataset
        self._validate_file_dpres_links(files)

        with file_error_collector() as collect_error:
            for file_metadata in files:
                # Validate metadata against JSON schema. The schema contains
                # properties introduced in JSON schema draft 7. Using
                # Draft7Validator ensures that older validators that do not
                # support draft 7 are not used, in which case part of the
                # schema would be ignored without any warning.

                validator = FileMetadataValidator(file_metadata, collect_error)
                validator.run_validations()

    def _validate_file_dpres_links(self, files: list[MetaxFile]) -> None:
        """
        Validate that dataset files contain every DPRES & non DPRES compatible
        file pair if any exist. Raises InvalidDatasetFileError:
        If any linked files are missing their DPRES and/or bit-level
        counterparts.

        :param files: Dataset files in a list
        """
        # Create DPRES compatibility links
        dpres_to_non_dpres = {
            f["pas_compatible_file"]: f["id"]
            for f in files
            if f.get("pas_compatible_file")
        }
        non_dpres_to_dpres = {
            f["non_pas_compatible_file"]: f["id"]
            for f in files
            if f.get("non_pas_compatible_file")
        }

        lone_dpres_ids = [
            id_
            for id_ in non_dpres_to_dpres.values()
            if id_ not in dpres_to_non_dpres
        ]
        lone_non_dpres_ids = [
            id_
            for id_ in dpres_to_non_dpres.values()
            if id_ not in non_dpres_to_dpres
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
                file_ for file_ in files if file_["id"] in all_lone_ids
            ]

            raise InvalidDatasetFileError(
                error, files=lone_files, is_dataset_error=True
            )


class FileMetadataValidator:
    """Validate the metadata of an individual file."""

    def __init__(
        self,
        file_metadata: MetaxFile,
        collect_error: Callable[[InvalidDatasetFileError], None],
    ) -> None:
        self.file_metadata = file_metadata
        self.collect_error = collect_error
        self.file_path = file_metadata.get("pathname", "")
        self.characteristics = file_metadata.get("characteristics") or {}
        self.characteristics_extension = (
            file_metadata.get("characteristics_extension") or {}
        )
        self.file_identifier = file_metadata.get("id")
        self.is_linked_bitlevel = bool(
            file_metadata.get("pas_compatible_file")
        )
        self.is_linked_pas_compatible = bool(
            file_metadata.get("non_pas_compatible_file")
        )

    def run_validations(self) -> None:
        """
        Validate file metadata. Raises `InvalidDatasetMetadataError` if any
        metadata is invalid.
        """

        self._check_file_format_version()
        self._check_draft7validator()
        self._check_file_path()
        self._check_grade()

    def _check_file_format_version(self) -> None:
        """
        Check that non bit-level file has a file format version and collect an
        error if it doesn't have it.
        """
        file_format_version = self.characteristics.get(
            "file_format_version", {}
        )
        file_format = (
            file_format_version.get("file_format", None)
            if file_format_version is not None
            else None
        )
        if not file_format and not self.is_linked_bitlevel:
            self.collect_error(
                InvalidFileMetadataError(
                    f"Non bit-level file must have `file_format_version` set: "
                    f"{self.file_path}",
                    files=[self.file_metadata],
                )
            )

    def _check_draft7validator(self) -> None:
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

    def _check_file_path(self) -> None:
        """
        Check that file path does not point outside SIP. If the check fails,
        collect the error.
        """
        normalized_path = os.path.normpath(self.file_path.strip("/"))
        if normalized_path.startswith(".."):
            self.collect_error(
                InvalidFileMetadataError(
                    f"The file path of file {self.file_identifier} is invalid:"
                    f" {self.file_path}",
                    files=[self.file_metadata],
                )
            )

    def _check_grade(self) -> None:
        """
        Check that files with `BIT_LEVEL_WITH_RECOMMENDED` or
        `UNACCEPTABLE` grade have a PAS compatible file. Collect the error.
        """

        grade = self.characteristics_extension.get("grade")
        if (
            grade in (BIT_LEVEL_WITH_RECOMMENDED, UNACCEPTABLE)
            and not self.is_linked_bitlevel
        ):
            self.collect_error(
                InvalidFileMetadataError(
                    f"File {self.file_identifier} with '{grade}' grade is "
                    f"not linked to a PAS compatible file",
                    files=[self.file_metadata],
                )
            )
        # Check that PAS compatible file has a good enough grade
        elif self.is_linked_pas_compatible and grade != RECOMMENDED:
            self.collect_error(
                InvalidFileMetadataError(
                    f"File {self.file_identifier} with grade '{grade}' marked "
                    f"as PAS compatible does not have the required "
                    f"'{RECOMMENDED}' grade",
                    files=[self.file_metadata],
                )
            )
