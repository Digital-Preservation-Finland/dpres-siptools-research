"""Packaging service exceptions."""

import contextlib

from typing import Iterator, Callable


MAX_FILE_ERROR_COUNT = 500


class InvalidDatasetError(Exception):
    """Exception raised when dataset is invalid."""


class InvalidDatasetFileError(InvalidDatasetError):
    """Exception raised when dataset contains invalid files."""

    def __init__(
            self,
            message: str,
            files: list[dict],
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


class InvalidContractMetadataError(InvalidDatasetError):
    """Exception raised when dataset metadata is invalid.

    SIP can not be created for dataset due to missing or invalid
    metadata.
    """


class InvalidFileError(InvalidDatasetFileError):
    """Exception raised when some files in dataset are invalid."""


class MissingFileError(InvalidDatasetFileError):
    """Exception raised when some files are not available."""


class WorkflowExistsError(Exception):
    """Exception raised when conflicting workflow exists."""

    def __init__(
        self, message: str = "Active workflow already exists for this dataset."
    ) -> None:
        super().__init__(message)


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
