import jsonschema

from siptools_research.exceptions import InvalidDatasetMetadataError
import siptools_research.schemas


class DirectoryValidation():
    """Motivation for this class is to speed up the process of directory tree
    validation fro dataset files
    """
    def __init__(self, metax_access):
        self.metax_client = metax_access
        self.valid_directories = set()

    def is_valid_for_file(self, file_md):
        """Validates the directory tree of the file.

        Raises ``InvalidDatasetMetadataError`` if directory tree is invalid.

        :param file_md: Metax file metadata dictionary
        :returns: ``None``
        """
        self._is_directory_valid(file_md['parent_directory']['identifier'])

    def _is_directory_valid(self, dir_identifier):
        """Checks recursively to top if directory tree is valid.

        :param dir_identifier: directory identifier of the directory to be
                               validated
        :returns: ``None``
        """

        if dir_identifier not in self.valid_directories:
            # Validate metadata against JSON schema
            dir_metadata = self.metax_client.get_directory(dir_identifier)
            try:
                jsonschema.validate(
                    dir_metadata,
                    siptools_research.schemas.DIRECTORY_METADATA_SCHEMA
                )
            except jsonschema.ValidationError as exc:
                directory_id = (dir_metadata['directory_path']
                                if 'directory_path' in dir_metadata
                                else dir_identifier)
                raise InvalidDatasetMetadataError(
                    "Validation error in metadata of {directory_id}: {error}"
                    .format(directory_id=directory_id, error=str(exc))
                )
            self.valid_directories.add(dir_identifier)
            if 'parent_directory' in dir_metadata:
                self._is_directory_valid(dir_metadata['parent_directory']
                                         ['identifier'])
