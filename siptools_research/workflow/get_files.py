"""Luigi task that gets files from Ida."""
import os
import logging

import luigi

from metax_access import Metax

from siptools_research.utils import ida, database as db
from siptools_research.workflowtask import WorkflowTask, InvalidMetadataError
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.workflow.validate_metadata import ValidateMetadata
from siptools_research.config import Configuration

# Print debug messages to stdout
logging.basicConfig(level=logging.DEBUG)


class UploadApiError(Exception):
    """Exception raised when files or mongo entries are missing from upload
    rest API.
    """
    pass


class GetFiles(WorkflowTask):
    """A task that reads file metadata from Metax and downloads the required
    files. Task requires that workspace directory exists and metadata is
    validated.

    Because output files are not known beforehand, a false target
    `get-files.finished` is created into workspace directory to notify luigi
    that this task has finished.

    Task requires that workspace directory is created and dataset metadata is
    validated.
    """
    success_message = 'Files were downloaded'
    failure_message = 'Could not get files'

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: list of required tasks
        """
        return [CreateWorkspace(workspace=self.workspace,
                                dataset_id=self.dataset_id,
                                config=self.config),
                ValidateMetadata(workspace=self.workspace,
                                 dataset_id=self.dataset_id,
                                 config=self.config)]

    def output(self):
        """The output that this Task produces.

        :returns: local target: ``task-getfiles.finished``
        :rtype: LocalTarget
        """
        return luigi.LocalTarget(
            os.path.join(self.workspace, "task-getfiles.finsihed")
        )

    def run(self):
        """Reads list of required files from Metax and downloads them.

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
        self._download_files(dataset_files, config_object)
        with self.output().open('w') as output:
            output.write("Dataset id=" + self.dataset_id)

    def _get_storage_path(self, identifier, fpath):
        """Returns the path to file with _id == identifier on passipservice"""

        files_col = db.Database(self.config).client.upload.files
        storage_path = files_col.find_one({"_id": identifier})
        if storage_path is None:
            raise UploadApiError("File %s not found in the database" % fpath)

        return files_col.find_one({"_id": identifier})["file_path"]

    def _download_files(self, dataset_files, config):
        """Reads and writes files on a path based on
        ``file_path`` in Metax

        :param dataset_files: list of files metadata dicts
        :param config: siptools_research config object
        :returns: ``None``
        """
        pas_storage_id = config.get("pas_storage_id")

        for dataset_file in dataset_files:
            identifier = dataset_file["identifier"]
            file_storage = dataset_file["file_storage"]["identifier"]

            # Full path to file
            target_path = os.path.normpath(
                os.path.join(
                    self.sip_creation_path,
                    dataset_file["file_path"].strip('/')
                )
            )
            if not target_path.startswith(self.sip_creation_path):
                raise InvalidMetadataError(
                    'The file path of file %s is invalid: %s' % (
                        identifier, dataset_file["file_path"]
                    )
                )

            # Create the download directory for file if it does not exist
            # already
            if not os.path.isdir(os.path.dirname(target_path)):
                # TODO: Use exist_ok -parameter when moving to python3
                os.makedirs(os.path.dirname(target_path))

            if file_storage == pas_storage_id:
                file_path = self._get_storage_path(
                    identifier, dataset_file["file_path"]
                )
                if not os.path.isfile(file_path):
                    raise UploadApiError("File %s not found on disk"
                                         % dataset_file["identifier"])
                # Create hard links to the files on workspace
                os.link(file_path, target_path)
            else:
                # Download file from IDA
                ida.download_file(
                    dataset_file['identifier'],
                    target_path,
                    self.config
                )
