"""Dataset class."""
import datetime
import enum
from pathlib import Path
import shutil

from siptools_research.config import Configuration
from siptools_research.exceptions import WorkflowExistsError
from siptools_research.metax import get_metax_client
import siptools_research.utils.database

PAS_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-pas"


def _timestamp():
    """Return time now.

    :returns: ISO 8601 string
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class Target(enum.Enum):
    """Target of a workflow."""

    METADATA_GENERATION = 'metadata_generation'
    VALIDATION = 'validation'
    PRESERVATION = 'preservation'


class Dataset:
    """Class for managing workflows of dataset."""

    def __init__(self, identifier, document=None,
                 config='/etc/siptools_research.conf'):
        """Initialize dataset."""
        self.identifier = identifier
        self._cached_document = document
        self._cached_metadata = None
        self.conf = Configuration(config)
        self.database = siptools_research.utils.database.Database(config)
        self._metax_client = get_metax_client(config)

    @property
    def _document(self):
        """Document in database."""
        if self._cached_document is None:
            self._cached_document \
                = self.database.get_document(self.identifier) or {}

        return self._cached_document

    @property
    def _metadata(self):
        """Dataset metadata from Metax."""
        if self._cached_metadata is None:
            self._cached_metadata \
                = self._metax_client.get_dataset(self.identifier)

        return self._cached_metadata

    @property
    def sip_identifier(self):
        """The SIP identifier of the dataset.

        The SIP identifier is the DOI of the dataset version which is in
        PAS data catalog.
        """
        if self._metadata['data_catalog']['identifier'] \
                == PAS_DATA_CATALOG_IDENTIFIER:
            # Dataset was originally created to PAS data catalog.
            identifier = self._metadata["preservation_identifier"]

        elif 'preservation_dataset_version' in self._metadata:
            # Dataset was created in IDA catalog, and it has been copied
            # to PAS data catalog. The preferred identifier of the PAS
            # version of the dataset will be used as SIP identifier.
            identifier = (self._metadata['preservation_dataset_version']
                          ['preferred_identifier'])
        else:
            # Dataset has not yet been copied to PAS data catalog.
            raise ValueError("The dataset has not been copied to PAS data"
                             "catalog, so DOI does not exist.")

        return identifier

    @property
    def preservation_state(self):
        """Preservation state of the dataset."""
        if 'preservation_dataset_version' in self._metadata:
            state = (self._metadata['preservation_dataset_version']
                     ['preservation_state'])
        else:
            state = self._metadata['preservation_state']

        return state

    def set_preservation_state(self, state, description):
        """Set preservation state of the dataset.

        If dataset has been copied to PAS data catalog, the preservation
        state of the PAS version is set.
        """
        if 'preservation_dataset_version' in self._metadata:
            preserved_dataset_id \
                = self._metadata['preservation_dataset_version']['identifier']
        else:
            preserved_dataset_id = self._metadata["identifier"]

        self._metax_client.set_preservation_state(preserved_dataset_id,
                                                  state,
                                                  description)

    @property
    def target(self):
        """Target of the workflow."""
        if "target" in self._document:
            target = Target(self._document["target"])
        else:
            target = None

        return target

    @property
    def workspace_root(self):
        """Root workspace directory."""
        # TODO: In TPASPKT-1269 <packaging_root>/file_cache was removed.
        # So <packagin_root> directory contains only "tmp" and
        # "workspaces" directories. Also "tmp" directory will be removed
        # in TPASPKT-648. Consider if "workspaces" directory is needed
        # anymore, or could workspaces be in <packaging_root>.
        packaging_root = self.conf.get('packaging_root')
        return Path(packaging_root) / "workspaces" / self.identifier

    @property
    def metadata_generation_workspace(self):
        """Return metadata generation workspace directory."""
        return self.workspace_root / Target.METADATA_GENERATION.value

    @property
    def validation_workspace(self):
        """Return validation workspace directory."""
        return self.workspace_root / Target.VALIDATION.value

    @property
    def preservation_workspace(self):
        """Return preservation workspace directory."""
        return self.workspace_root / Target.PRESERVATION.value

    @property
    def enabled(self):
        """Check if dataset has active workflow."""
        return self._document.get('enabled', False)

    def disable(self):
        """Disable workflow."""
        self._cached_document = self.database.update_document(
            self.identifier,
            {'enabled': False}
        )

    def enable(self):
        """Enable workflow."""
        self._cached_document = self.database.update_document(
            self.identifier,
            {'enabled': True}
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
        """Set target of workflow.

        param Target target: The target of the workflow
        """
        self._cached_document = self.database.update_document(
            self.identifier,
            {
                'target': target.value,
                'enabled': True
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
            _delete_directory(path)

        self.metadata_generation_workspace.mkdir(parents=True)

        self._set_target(Target.METADATA_GENERATION)

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
            _delete_directory(path)

        self.validation_workspace.mkdir(parents=True)

        self._set_target(Target.VALIDATION)

    def preserve(self):
        """Preserve dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                'Active workflow already exists for this dataset.'
            )

        # Clear the preservation workspace
        _delete_directory(self.preservation_workspace)

        self.preservation_workspace.mkdir(parents=True)

        self._set_target(Target.PRESERVATION)


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
        search['enabled'] = enabled
    if target is not None:
        search['target'] = target
    if identifier is not None:
        search['_id'] = identifier

    database = siptools_research.utils.database.Database(config)
    return list(
        Dataset(document['_id'], document=document, config=config)
        for document
        in database.find(search)
    )


def _delete_directory(path):
    """Delete directory if it exists.

    :param path: Directory path
    """
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        # Previous workspace does not exist
        pass
