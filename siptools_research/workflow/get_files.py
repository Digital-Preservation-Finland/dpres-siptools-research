"""Luigi task that gets files from Ida."""
import os
import shutil
try:
    from tempfile import TemporaryDirectory
except ImportError:
    # TODO: Remove this when Python 2 support can be dropped
    from siptools_research.temporarydirectory import TemporaryDirectory

import luigi
from metax_access import Metax
import upload_rest_api.database

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

    Because output files are not known beforehand, a false target
    `get-files.finished` is created into workspace directory to notify
    luigi that this task has finished.

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

        :returns: local target: ``task-getfiles.finished``
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, "task-getfiles.finsihed")
        )

    def run(self):
        """Read list of required files from Metax and download them.

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

        # get files from ida or pas storage
        self._download_files(dataset_files)
        with self.output().open('w') as output:
            output.write("Dataset id=" + self.dataset_id)

    def _download_files(self, dataset_files):
        """Download dataset files.

        Files are written to path based on ``file_path`` in Metax.

        :param dataset_files: list of files metadata dicts
        :returns: ``None``
        """
        upload_database = upload_rest_api.database.Database()

        config_object = Configuration(self.config)
        tmp = os.path.join(config_object.get('packaging_root'), 'tmp/')
        with TemporaryDirectory(prefix=tmp) as temporary_directory:
            for dataset_file in dataset_files:
                identifier = dataset_file["identifier"]

                # Full path to file
                target_path = os.path.normpath(
                    os.path.join(
                        temporary_directory,
                        dataset_file["file_path"].strip('/')
                    )
                )
                if not target_path.startswith(temporary_directory):
                    raise InvalidFileMetadataError(
                        'The file path of file %s is invalid: %s' % (
                            identifier, dataset_file["file_path"]
                        )
                    )

                # Create the download directory for file if it does not
                # exist already
                if not os.path.isdir(os.path.dirname(target_path)):
                    # TODO: Use exist_ok -parameter when moving to
                    # python3
                    os.makedirs(os.path.dirname(target_path))

                download_file(
                    dataset_file, target_path, self.config, upload_database
                )

            # Move files to SIP directory when all files have been
            # succesfully downloaded
            for file_ in os.listdir(temporary_directory):
                shutil.move(os.path.join(temporary_directory, file_),
                            self.sip_creation_path)
