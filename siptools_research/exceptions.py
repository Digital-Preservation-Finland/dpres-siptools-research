"""Packaging service exceptions."""


class InvalidDatasetError(Exception):
    """Exception raised when dataset is invalid."""


class InvalidSIPError(InvalidDatasetError):
    """Exception raised when the SIP is invalid.

    SIP created from dataset is rejected in digital preservation service.
    """


class InvalidDatasetMetadataError(InvalidDatasetError):
    """Exception raised when dataset metadata is invalid.

    SIP can not be created for dataset due to missing or invalid metadata.
    """


class InvalidFileMetadataError(InvalidDatasetError):
    """Exception raised when file metadata is invalid.

    SIP can not be created for dataset due to missing or invalid metadata.
    """


class InvalidContractMetadataError(InvalidDatasetError):
    """Exception raised when dataset metadata is invalid.

    SIP can not be created for dataset due to missing or invalid metadata.
    """


class InvalidFileError(InvalidDatasetError):
    """Exception raised when some files in dataset are invalid."""

    def __init__(self, message, files=None):
        """Initialize exception.

        :param message: Error message
        :param files: List of invalid files
        """
        super(InvalidFileError, self).__init__(message)
        self.files = files


class MissingFileError(InvalidDatasetError):
    """Exception raised when some files are not available."""

    def __init__(self, message, files=None):
        """Initialize exception.

        :param message: Error message
        :param files: List of missing files
        """
        super(MissingFileError, self).__init__(message)
        self.files = files
