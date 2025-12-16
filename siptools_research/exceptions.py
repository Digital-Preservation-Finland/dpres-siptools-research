"""Packaging service exceptions."""

from __future__ import annotations
import contextlib
from typing import Iterator, Callable

from metax_access.response import MetaxFile


MAX_FILE_ERROR_COUNT = 500


class InvalidDatasetError(Exception):
    """Exception raised when dataset is invalid."""


class InvalidDatasetFileError(InvalidDatasetError):
    """Exception raised when dataset contains invalid files."""

    def __init__(
            self,
            message: str,
            files: list[dict] | list[MetaxFile],
            is_dataset_error: bool = False):
        """Initialize exception.

        :param message: Error message
        :param files: List of invalid files
        :param is_dataset_error: Whether the error is due to the dataset.
            Default is False as most errors are
            expected to be caused by invalid file metadata only.
        """
        super().__init__(message)
        self.files = files
        self.is_dataset_error = is_dataset_error


class InvalidSIPError(InvalidDatasetError):
    """Exception raised when the SIP is invalid.

    SIP created from dataset is rejected in digital preservation
    service.
    """


class InvalidDatasetMetadataError(InvalidDatasetError):
    """Exception raised when dataset metadata is invalid.

    SIP can not be created for dataset due to missing or invalid
    metadata.
    """


class InvalidFileMetadataError(InvalidDatasetFileError):
    """Exception raised when file metadata is invalid.

    SIP can not be created for dataset due to missing or invalid
    file metadata.
    """


class InvalidFileError(InvalidDatasetFileError):
    """Exception raised when some files in dataset are invalid."""


class MissingFileError(InvalidDatasetFileError):
    """Exception raised when some files are not available."""


class ActionNotAllowedError(Exception):
    """Exception raised when action is not allowed for the dataset."""

    _default_message = "Action not allowed for unknown reason"

    def __init__(self, message: str | None = None) -> None:
        """Initialize the exception with an optional custom message."""
        super().__init__(message or self._default_message)


class MetadataNotGeneratedError(ActionNotAllowedError):
    """Raised when metadata has not been generated.

    Technical metadata must generated for files so that it can be added
    to METS.
    """

    _default_message = "Metadata has not been generated."


class MetadataNotConfirmedError(ActionNotAllowedError):
    """Raised when metadata has not been confirmed.

    The user must confirm the generated metadata, before it is
    preserved.
    """

    _default_message = "Metadata has not been confirmed."


class WorkflowExistsError(ActionNotAllowedError):
    """Raised when conflicting workflow exists.

    If a workflow is already running for a dataset, another workflow may
    not be started.
    """

    _default_message = "Active workflow already exists for this dataset"


class AlreadyPreservedError(ActionNotAllowedError):
    """Raised when dataset already is in preservation.

    If a dataset is already in preservation, it can not be preserved
    again. Note that cumulative dataset might have to be preserved
    again, but preserving cumulative datasets is not yet supported.
    """

    _default_message = "Dataset is already preserved."


class CopiedToPasDataCatalogError(ActionNotAllowedError):
    """Raised when dataset has been copied to PAS data catalog.

    During the preservation process of Ida datasets, the dataset
    metadata is copied from Ida data catalog to PAS data catalog. Once
    the dataset has been copied, it can not be undone, and therefore the
    dataset can not be unlocked before it is in preservation.

    TODO: This exception might be unncecessary when preserving dataset
    in "draft mode" is implemented. See TPASPKT-1167 for more
    information.
    """

    _default_message = "Dataset has already been copied to PAS data catalog."


class NotProposedError(ActionNotAllowedError):
    """Raised when dataset is not yet proposed.

    The dataset must be proposed for preservation before the proposal
    can be rejected or accepted.
    """

    _default_message = "Dataset has not been proposed for preservation."


class BulkInvalidDatasetFileError(InvalidDatasetError):
    """Exception consisting of multiple file errors"""
    def __init__(
            self, message: str, file_errors: list[InvalidDatasetFileError]):
        """Initialize exception.

        :param file_errors: Error message
        """
        super().__init__(message)

        self.file_errors = file_errors


@contextlib.contextmanager
def file_error_collector(max_count: int = MAX_FILE_ERROR_COUNT) -> \
        Iterator[Callable[[InvalidDatasetFileError], None]]:
    """
    Context manager to collect file errors. The `max_count` is used to limit
    the amount of files with errors per invocation and will take care of
    raising the underlying the exception when appropriate:

    * If n < max_count files with errors are collected, the bulk error is
      raised at the end of the context manager run
    * If `max_count` files with errors are collected, the execution of the
      workflow task is halted and the execution is raised immediately; this is
      done to avoid unnecessary work for datasets where the bulk of files are
      likely to be invalid.

    The context manager will yield a collector function that accepts a single
    `InvalidDatasetFileError` exception.

    :raises BulkInvalidDatasetFileError: If any errors were collected during
    the run
    """
    collected_errors = []
    file_ids = set()

    def collect(error: InvalidDatasetFileError):
        collected_errors.append(error)

        for file in error.files:
            file_ids.add(file["id"])

        if len(file_ids) >= max_count:
            # The maximum amount of file errors has been reached; halt
            # and raise the exception immediately
            raise BulkInvalidDatasetFileError(
                message=(
                    f"{len(collected_errors)} errors in {len(file_ids)} files "
                    "found before processing was halted"
                ),
                file_errors=collected_errors
            )

    yield collect

    # Check if any errors were collected and raise the exception if so
    if collected_errors:
        raise BulkInvalidDatasetFileError(
            message=(
                f"{len(collected_errors)} errors in {len(file_ids)} files "
                "found during processing"
                if len(collected_errors) > 1 else str(collected_errors[0])
            ),
            file_errors=collected_errors
        )
