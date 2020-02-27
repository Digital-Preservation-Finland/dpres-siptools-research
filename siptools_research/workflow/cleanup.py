"""Luigi task that removes workspaces of finished workflows.
"""
import os
import shutil

import luigi
from metax_access import Metax
from metax_access.metax import DatasetNotFoundError

from siptools_research.utils.database import Database
from siptools_research.config import Configuration
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.report_preservation_status import (
    ReportPreservationStatus
)


class CleanupWorkspace(WorkflowTask):
    """Removes the workspace when it is ready for cleanup. Task requires that
    preservation status has been reported.
    """
    success_message = 'Workspace was cleaned'
    failure_message = 'Cleaning workspace failed'

    def file_cache_cleaned(self):
        """Check if all the files are removed from file cache

        :returns: Boolean
        """
        identifiers, cache_path = self.get_identifiers()

        for identifier in identifiers:
            filepath = os.path.join(cache_path, identifier)
            if os.path.isfile(filepath):
                return False

        return True

    def clean_file_cache(self):
        """Remove cached files"""
        identifiers, cache_path = self.get_identifiers()

        for identifier in identifiers:
            filepath = os.path.join(cache_path, identifier)
            if os.path.isfile(filepath):
                os.remove(filepath)

    def get_identifiers(self):
        """Return a list of all the file identifiers and the path to the
        downloaded files.

        :returns: Tuple (list of identifiers, cache_path)
        """
        config_object = Configuration(self.config)
        packaging_root = config_object.get("packaging_root")
        cache_path = os.path.join(packaging_root, "file_cache")

        metax_client = Metax(
            config_object.get('metax_url'),
            config_object.get('metax_user'),
            config_object.get('metax_password'),
            verify=config_object.getboolean('metax_ssl_verification')
        )
        try:
            dataset_files = metax_client.get_dataset_files(self.dataset_id)
            return [_file["identifier"] for _file in dataset_files], cache_path
        except DatasetNotFoundError:
            return [], cache_path

    def requires(self):
        """The Tasks that this Task depends on.

        :returns: ReportPreservationStatus task
        """
        return ReportPreservationStatus(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)

    def run(self):
        """Removes a finished workspace.

        :returns: None
        """
        shutil.rmtree(self.workspace)
        self.clean_file_cache()

    def complete(self):
        """Task is complete when workspace does not exist, but
        ReportPreservationStatus has finished according to workflow database.

        :returns: ``True`` or ``False``
        """

        # Check if ReportPreservationStatus has finished
        database = Database(self.config)
        try:
            result = database.get_event_result(self.document_id,
                                               'ReportPreservationStatus')

        # TODO: Maybe these exceptions should be handled by Database module?
        except KeyError:
            # ReportPreservationStatus has not run yet
            return False

        except TypeError:
            # Workflow is not found in database
            return False

        if result != 'success':
            return False

        # Check are the cached files cleaned and if workspace exists
        return self.file_cache_cleaned() and not os.path.exists(self.workspace)


@CleanupWorkspace.event_handler(luigi.Event.SUCCESS)
def report_workflow_completion(task):
    """Report completion of workflow to workflow database.

    :param task: CleanupWorkspace object
    :returns: ``None``
    """
    database = Database(task.config)
    database.set_completed(task.document_id)
