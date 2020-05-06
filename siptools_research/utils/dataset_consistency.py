from siptools_research.workflowtask import InvalidMetadataError


class DatasetConsistency(object):
    """Motivation for this class is to speed up the process of checking if
    the files returned by Metax API datasets/datasetid/files are contained
    by Metax dataset files or directories attributes"""

    def __init__(self, metax_access, dataset):
        self.metax_client = metax_access
        self.dataset = dataset
        # directories is a set object which contains directory identifiers
        # contained by the dataset. Whenever a new directory is found it is
        # added to this set during the is_consistent_for_file() function call.
        self.directories = set()

    def is_consistent_for_file(self, file_md):
        """Verifies that file is contained by dataset files or directories.
        If not, raises `InvalidMetadataError`.

        :param file_md: Metax file metadata dictionary
        :returns: ``None`` if file contained by dataset. Otherwise raises
            `InvalidMetadataError`
        """
        file_identifier = file_md['identifier']
        if 'files' in self.dataset['research_dataset']:
            for file_ in self.dataset['research_dataset']['files']:
                if file_['identifier'] == file_identifier:
                    return
        if file_md['parent_directory']['identifier'] not in self.directories:
            temp_dirs = set()
            if not self._is_directory_contained_by_dataset_directories(
                    file_md['parent_directory']['identifier'], temp_dirs):
                raise InvalidMetadataError(
                    'File not found from dataset files nor directories: %s'
                    % (file_md["file_path"])
                )
            # we might have found new dataset directories
            self.directories.update(temp_dirs)

    def _is_directory_contained_by_dataset_directories(self, identifier,
                                                       temp_dirs):
        """Checks recursively if directory is contained by dataset
        directories, if any.

        :param identifier: directory identifier to be checked
        :param temp_dirs: a set object containing directory hierarchy during
            recursion. In return it is empty if the directory is not contained
            by the dataset. Otherwise contains the set of
            directories(i.e directory hierarchy) contained by the dataset.
        :returns: ``True`` if file contained by dataset directory
        otherwise ``False``
        """

        if 'directories' in self.dataset['research_dataset']:
            for directory in self.dataset['research_dataset']['directories']:
                if directory['identifier'] == identifier:
                    temp_dirs.add(identifier)
                    return True

            dire = self.metax_client.get_directory(identifier)

            if 'parent_directory' in dire:
                temp_dirs.update(
                    [identifier,
                     dire['parent_directory']['identifier']]
                )
                return self._is_directory_contained_by_dataset_directories(
                    dire['parent_directory']['identifier'], temp_dirs
                )
        temp_dirs.clear()
        return False
