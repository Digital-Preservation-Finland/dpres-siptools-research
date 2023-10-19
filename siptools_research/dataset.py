"""Dataset class."""
import datetime
from pathlib import Path
import shutil

from siptools_research.config import Configuration
from siptools_research.exceptions import WorkflowExistsError
import siptools_research.utils.database


def _timestamp():
    """Return time now.

    :returns: ISO 8601 string
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class Dataset:
    """Class for managing workflows of dataset."""

    def __init__(self, identifier, document=None,
                 config='/etc/siptools_research.conf'):
        """Initialize dataset."""
        self.identifier = identifier
        self._cached_document = document
        self.conf = Configuration(config)
        self.database = siptools_research.utils.database.Database(config)

    @property
    def _document(self):
        """Document in database."""
        if self._cached_document is None:
            self._cached_document \
                = self.database.get_document(self.identifier) or {}

        return self._cached_document

    @property
    def target(self):
        """Target of workflow."""
        return self._document.get('target', None)

    @property
    def workspace_root(self):
        """Root workspace directory."""
        packaging_root = self.conf.get('packaging_root')
        return Path(packaging_root) / "workspaces" / self.identifier

    @property
    def metadata_generation_workspace(self):
        """Return metadata generation workspace directory."""
        return self.workspace_root / "metadata_generation"

    @property
    def validation_workspace(self):
        """Return validation workspace directory."""
        return self.workspace_root / "validation"

    @property
    def preservation_workspace(self):
        """Return preservation workspace directory."""
        return self.workspace_root / "preservation"

    @property
    def sip_creation_path(self):
        """Return preservation workspace directory."""
        return self.preservation_workspace / "sip-in-progress"

    @property
    def enabled(self):
        """Check if dataset has active workflow."""
        return not self._document.get('disabled', True)

    def disable(self):
        """Disable workflow."""
        self._cached_document = self.database.update_document(
            self.identifier,
            {'disabled': True}
        )

    def enable(self):
        """Enable workflow."""
        self._cached_document = self.database.update_document(
            self.identifier,
            {'disabled': False}
        )

    # TODO: These task logs are not required anymore for anything else
    # than debugging purposes. Is anyone really using them, or could
    # they be removed? The luigi logs contain more or less the same
    # information.
    def log_task(self, task_name, result, message):
        """Log workflow event.

        :param task_name: Name of the task
        :param result: Result string ('failure' or 'success')
        :param messages: Information about the task
        :returns: ``None``
        """
        self._cached_document = self.database.update_document(
            self.identifier,
            {
                'workflow_tasks.' + task_name: {
                    'timestamp': _timestamp(),
                    'messages': message,
                    'result': result
                }
            }
        )

    def get_tasks(self):
        """Get dictionary of events."""
        return self._document.get('workflow_tasks', {})

    def get_task_timestamp(self, task_name):
        """Read task timestamp for a task.

        :param task_name: Name of task
        :returns: Task timestamp
        """
        if not self._document:
            raise ValueError
        return self._document['workflow_tasks'][task_name]['timestamp']

    def _set_target(self, target):
        """Set target of workflow."""
        self._cached_document = self.database.update_document(
            self.identifier,
            {
                'target': target,
                'disabled': False
            }
        )

    def generate_metadata(self):
        """Generate dataset metadata.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                'Active workflow already exists for this dataset.'
            )

        # Clear the workspaces
        for path in [self.metadata_generation_workspace,
                     self.validation_workspace,
                     self.preservation_workspace]:
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                # Previous workspace does not exist
                pass

        self.metadata_generation_workspace.mkdir(parents=True)

        self._set_target('metadata_generation')

    def validate(self):
        """Validate metadata and files of dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                'Active workflow already exists for this dataset.'
            )

        # Clear the workspaces
        for path in [self.validation_workspace, self.preservation_workspace]:
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                # Previous workspace does not exist
                pass
        self.validation_workspace.mkdir(parents=True)

        self._set_target('validation')

    def preserve(self):
        """Preserve dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                'Active workflow already exists for this dataset.'
            )

        # Clear the preservation workspace
        try:
            shutil.rmtree(self.preservation_workspace)
        except FileNotFoundError:
            # Previous workspace does not exist
            pass
        self.preservation_workspace.mkdir(parents=True)
        self.sip_creation_path.mkdir()

        self._set_target('preservation')


def find_datasets(enabled=None,
                  target=None,
                  identifier=None,
                  config='/etc/siptools_research.conf'):
    """Find datasets by search criteria.

    :param bool enabled: If `True`, show only enabled datasets, if
                         `False`, show only disabled datasets.
    :param str target: Show only datasets with this target
    :param str identifier: Show only dataset with this identifier
    :param str config: Path to configuration file
    :returns: List of matching datasets
    """
    search = {}
    if enabled is not None:
        search['disabled'] = not enabled
    if target is not None:
        search['target'] = target
    if identifier is not None:
        search['identifier'] = identifier

    database = siptools_research.utils.database.Database(config)
    return list(
        Dataset(document['_id'], document=document, config=config)
        for document
        in database.find(search)
    )
