"""Luigi task that gets files from Ida."""
import os

import luigi
from metax_access import Metax

from siptools_research.utils.download import download_file
from siptools_research.workflowtask import WorkflowTask
from siptools_research.exceptions import InvalidFileMetadataError
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration


class GetFiles(WorkflowTask):
    """A task that reads downloads the dataset files to workspace.

    Task requires that workspace directory exists and metadata is
    validated.

    Task requires that workspace directory is created and dataset
    metadata is validated.
    """

    success_message = 'Files were downloaded'
    failure_message = 'Could not get files'

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """Return the output target of this Task.

        :returns: `<workspace>/dataset_files`
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, "dataset_files")
        )

    def run(self):
        """Read list of required files from Metax and download them.

        Files are written to path based on ``file_path`` in Metax.

        :returns: ``None``
        """
        # Find file identifiers from Metax dataset metadata.
        config_object = Configuration(self.config)
        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        dataset_files = metax_client.get_dataset_files(self.dataset_id)

        # Download files to temporary target directory which will be
        # moved to output target path when all files have been
        # downloaded
        with self.output().temporary_path() as target_path:
            os.mkdir(target_path)

            for dataset_file in dataset_files:
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
                        'The file path of file {} is invalid: {}'.format(
                            identifier, dataset_file["file_path"]
                        )
                    )

                # Create the download directory for file if it does not exist
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                download_file(
                    file_metadata=dataset_file,
                    dataset_id=self.dataset_id,
                    linkpath=full_path,
                    config_file=self.config
                )
