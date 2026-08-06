"""Luigi task that gets files from Ida."""
import logging
import os

import luigi

from siptools_research.exceptions import (InaccessibleIDAFileError,
                                          InvalidFileMetadataError,
                                          MissingFileError)
from siptools_research.metax import get_metax_client
from siptools_research.download import (download_file,
                                        FileNotAvailableError,
                                        IDAFileAccessError)
from siptools_research.workflowtask import WorkflowTask


class GetFiles(WorkflowTask):
    """Downloads the dataset files.

    Does not require anything.

    Downloads files to `dataset_files` directory in metadata generation
    workspace.
    """

    success_message = 'Files were downloaded'
    failure_message = 'Could not get files'

    def output(self):
        return luigi.LocalTarget(
            str(self.workspace.metadata_generation / "dataset_files")
        )

    def run(self):
        # Create file cache directory. To avoid unnecessary downloads,
        # files are saved to file cache directory, which is not deleted
        # when any workflow is restarted.
        file_cache = self.workspace.root / "file_cache"
        file_cache.mkdir(exist_ok=True)

        # Find file identifiers from Metax dataset metadata.
        files \
            = get_metax_client(self.config).get_dataset_files(self.dataset_id)

        # Download files to temporary target directory which will be
        # moved to output target path when all files have been
        # downloaded
        missing_files = []
        ida_fault_files = []
        with self.output().temporary_path() as target_path:
            os.mkdir(target_path)

            for dataset_file in files:
                identifier = dataset_file["id"]

                # Full path to file. Path normalization should not be
                # required, but it is done just in case.
                full_path = os.path.normpath(
                    os.path.join(
                        target_path,
                        dataset_file["pathname"].strip('/')
                    )
                )
                if not full_path.startswith(target_path):
                    raise InvalidFileMetadataError(
                        f'The file path of file {identifier} is invalid:'
                        f' {dataset_file["pathname"]}',
                        files=[dataset_file]
                    )

                # Create the download directory for file if it does not
                # exist
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                cached_file = file_cache / identifier
                if not cached_file.exists():
                    try:
                        download_file(
                            file_metadata=dataset_file,
                            path=cached_file,
                            config_file=self.config
                        )
                    except IDAFileAccessError:
                        ida_fault_files.append(dataset_file)
                        continue
                    except FileNotAvailableError:
                        missing_files.append(dataset_file)
                        continue

                os.link(cached_file, full_path)

            if ida_fault_files:
                logging.error(
                    f"IDA files in {dataset_file['csc_project']} are not "
                    "accessible. Mount configuration might be broken or "
                    "incomplete!"
                )
                raise InaccessibleIDAFileError(
                    "IDA files could not be processed due to a fault with the "
                    "IDA service. This is not an user error; service "
                    "administration is investigating the issue.",
                    ida_fault_files
                )
            if missing_files:
                # TODO: If files are missing, there is something wrong
                # in Ida, Metax, or upload-rest-api. The user can not do
                # anything about it. Currently MissingFileError is
                # raised which means that the workflow is disabled and
                # the dataset will be marked invalid, which is
                # misleading for the user.
                raise MissingFileError(
                    f"{len(missing_files)} files are missing. "
                    "This is not an user error; service administration is "
                    "investigating the issue.", missing_files
                )
