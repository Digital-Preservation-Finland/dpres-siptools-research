"""Luigi task that gets files from Ida."""
import os

import luigi

from siptools_research.exceptions import (InvalidFileMetadataError,
                                          MissingFileError)
from siptools_research.metax import get_metax_client
from siptools_research.utils.download import (download_file,
                                              FileNotAvailableError)
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
            str(self.dataset.metadata_generation_workspace / "dataset_files")
        )

    def run(self):
        # Find file identifiers from Metax dataset metadata.
        files \
            = get_metax_client(self.config).get_dataset_files(self.dataset_id)

        # Download files to temporary target directory which will be
        # moved to output target path when all files have been
        # downloaded
        missing_files = []
        with self.output().temporary_path() as target_path:
            os.mkdir(target_path)

            for dataset_file in files:
                identifier = dataset_file["identifier"]

                # Full path to file
                full_path = os.path.normpath(
                    os.path.join(
                        target_path,
                        dataset_file["file_path"].strip('/')
                    )
                )
                if not full_path.startswith(target_path):
                    raise InvalidFileMetadataError(
                        f'The file path of file {identifier} is invalid:'
                        f' {dataset_file["file_path"]}'
                    )

                # Create the download directory for file if it does not
                # exist
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                try:
                    download_file(
                        file_metadata=dataset_file,
                        dataset_id=self.dataset_id,
                        linkpath=full_path,
                        config_file=self.config
                    )
                except FileNotAvailableError:
                    missing_files.append(dataset_file['identifier'])

            if missing_files:
                raise MissingFileError(
                    f"{len(missing_files)} files are missing", missing_files
                )
