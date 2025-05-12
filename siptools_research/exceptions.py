"""Packaging service exceptions."""


class InvalidDatasetError(Exception):
    """Exception raised when dataset is invalid."""


class InvalidDatasetFileError(InvalidDatasetError):
    """Exception raised when dataset contains invalid files."""

    def __init__(self, message, files, is_dataset_error=False):
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
