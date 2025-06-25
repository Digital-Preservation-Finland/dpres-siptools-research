"""Dataset class."""

import datetime
import enum
import shutil
from pathlib import Path

from metax_access import DS_STATE_INITIALIZED

from siptools_research.config import Configuration
from siptools_research.database import connect_mongoengine
from siptools_research.exceptions import WorkflowExistsError
from siptools_research.metax import get_metax_client
from siptools_research.models.dataset_entry import (DatasetWorkflowEntry,
                                                    TaskEntry)
from siptools_research.models.file_error import FileError

PAS_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-pas"
IDA_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-ida"


def _timestamp():
    """Return time now.

    :returns: ISO 8601 string
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class Target(enum.Enum):
    """Target of a workflow."""

    METADATA_GENERATION = "metadata_generation"
    VALIDATION = "validation"
    PRESERVATION = "preservation"


class Dataset:
    """Class for managing workflows of dataset."""

    def __init__(
        self, identifier, document=None, config="/etc/siptools_research.conf"
    ):
        """Initialize dataset."""
        self.identifier = identifier
        self._cached_metadata = None
        self.conf = Configuration(config)
        self._metax_client = get_metax_client(config)

        connect_mongoengine(config)

        self._document = document
        if self._document is None:
            try:
                self._document = \
                    DatasetWorkflowEntry.objects.get(id=self.identifier)
            except DatasetWorkflowEntry.DoesNotExist:
                self._document = DatasetWorkflowEntry(
                    id=self.identifier
                )

    @property
    def _metadata(self):
        """Dataset metadata from Metax."""
        if self._cached_metadata is None:
            self._cached_metadata = self._metax_client.get_dataset(
                self.identifier
            )

        return self._cached_metadata

    @property
    def _has_been_copied_to_pas_datacatalog(self):
        return self._metadata["preservation"]["dataset_version"]["id"]\
            is not None

    @property
    def sip_identifier(self):
        """The SIP identifier of the dataset.

        The SIP identifier is the DOI of the dataset version which is in
        PAS data catalog.
        """
        if self._metadata["data_catalog"] == PAS_DATA_CATALOG_IDENTIFIER:
            # Dataset was originally created to PAS data catalog.
            identifier = self._metadata["persistent_identifier"]

        elif self._metadata["data_catalog"] == IDA_DATA_CATALOG_IDENTIFIER:

            if self._has_been_copied_to_pas_datacatalog:
                # Dataset was created in IDA catalog, and it has been
                # copied to PAS data catalog. The preferred identifier
                # of the PAS version of the dataset will be used as SIP
                # identifier.
                identifier = (self._metadata["preservation"]["dataset_version"]
                              ["persistent_identifier"])
            else:
                # Dataset has not yet been copied to PAS data catalog.
                error = ("The dataset has not been copied to PAS data"
                         "catalog, so DOI does not exist.")
                raise ValueError(error)
        else:
            error = f"Unknown data catalog: {self._metadata['data_catalog']}"
            raise ValueError(error)

        return identifier

    @property
    def preservation_state(self):
        """Preservation state of the dataset."""
        if self._has_been_copied_to_pas_datacatalog:
            return self._metadata["preservation"]["dataset_version"][
                "preservation_state"
            ]
        else:
            return self._metadata["preservation"]["state"]

    def set_preservation_reason(self, reason: str):
        """Set preservation reason description of the dataset."""
        self._metax_client.set_preservation_reason(self.pas_dataset_id, reason)

    def set_preservation_state(self, state, description):
        """Set preservation state of the dataset.

        If dataset has been copied to PAS data catalog, the preservation
        state of the PAS version is set.
        """
        if self._has_been_copied_to_pas_datacatalog:
            preserved_dataset_id = self._metadata["preservation"][
                "dataset_version"]["id"]
        else:
            preserved_dataset_id = self._metadata["id"]

        self._metax_client.set_preservation_state(
            preserved_dataset_id, state, description
        )

    def mark_preserved(self):
        """Mark dataset preserved.

        Sets `pas_package_created` value to True, so other services know
        that the dataset is in digital preservation.

        If dataset has been copied to PAS data catalog, the value is
        updated in PAS version, not in the original version.
        """
        if self._has_been_copied_to_pas_datacatalog:
            preserved_dataset_id = self._metadata["preservation"][
                "dataset_version"]["id"]
        else:
            preserved_dataset_id = self._metadata["id"]

        self._metax_client.set_pas_package_created(preserved_dataset_id)

    def unlock(self):
        """Unlock dataset."""
        self._metax_client.unlock_dataset(self.identifier)

        dataset_files = self._metax_client.get_dataset_files(self.identifier)

        # Delete generic file errors and those related to this dataset.
        # User is able to change metadata once the dataset and its files are
        # unlocked, which would make the errors invalid.
        FileError.objects.filter(
            file_id__in=[file["id"] for file in dataset_files],
            dataset_id__in=(None, self.identifier)
        ).delete()

    def copy_to_pas_datacatalog(self):
        """Copy dataset to PAS data catalog."""
        if self._metadata["data_catalog"] != PAS_DATA_CATALOG_IDENTIFIER \
                and not self._has_been_copied_to_pas_datacatalog:
            self._metax_client.copy_dataset_to_pas_catalog(self.identifier)

    def get_datacite(self) -> bytes:
        """Get Datacite document for dataset's PAS data catalog copy"""
        return self._metax_client.get_datacite(self.pas_dataset_id)

    @property
    def pas_dataset_id(self):
        """Dataset identifier of the dataset using the PAS data catalog.

        If the dataset was created in IDA, this is the same as the copy
        created using :meth:`copy_to_pas_datacatalog`. If not, this is the
        dataset identifier itself.
        """
        return (
            self._metadata["preservation"]["dataset_version"]["id"]
            or self._metadata["id"]
        )

    @property
    def target(self):
        """Target of the workflow."""
        if self._document.target:
            target = Target(self._document["target"])
        else:
            target = None

        return target

    @property
    def workspace_root(self):
        """Root workspace directory."""
        packaging_root = self.conf.get("packaging_root")
        # Currently packaging_root contains only the workspace
        # directories, but it might have other purposes in future, so
        # workspaces are created in "workspaces" subdirectory.
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
        return self._document.enabled

    def disable(self):
        """Disable workflow."""
        self._document.enabled = False
        self._document.save()

    def enable(self):
        """Enable workflow."""
        self._document.enabled = True
        self._document.save()

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
        self._document.workflow_tasks[task_name] = TaskEntry(
            timestamp=_timestamp(),
            messages=message,
            result=result
        )
        self._document.save()

    def get_tasks(self):
        """Get dictionary of events."""
        return self._document.workflow_tasks

    def get_task_timestamp(self, task_name):
        """Read task timestamp for a task.

        :param task_name: Name of task
        :returns: Task timestamp
        """
        if not self._document:
            raise ValueError
        return self._document["workflow_tasks"][task_name]["timestamp"]

    def _set_target(self, target):
        """Set target of workflow.

        param Target target: The target of the workflow
        """
        self._document.target = target.value
        self._document.enabled = True
        self._document.save()

    def generate_metadata(self):
        """Generate dataset metadata.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                "Active workflow already exists for this dataset."
            )

        # Clear the workspaces
        for path in [
            self.metadata_generation_workspace,
            self.validation_workspace,
            self.preservation_workspace,
        ]:
            _delete_directory(path)

        self.metadata_generation_workspace.mkdir(parents=True)

        self._set_target(Target.METADATA_GENERATION)

    def validate(self):
        """Validate metadata and files of dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError(
                "Active workflow already exists for this dataset."
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
                "Active workflow already exists for this dataset."
            )

        # Clear the preservation workspace
        _delete_directory(self.preservation_workspace)

        self.preservation_workspace.mkdir(parents=True)

        self._set_target(Target.PRESERVATION)

    def reset(self, description: str, reason_description: str) -> None:
        """Reset dataset.

        This sets its state to INITIALIZED and removes any existing file
        errors.
        """
        if self.enabled:
            raise WorkflowExistsError(
                "Active workflow already exists for this dataset."
            )

        self.set_preservation_state(DS_STATE_INITIALIZED, description)
        self.set_preservation_reason(reason_description)

        # Unlock dataset
        self.unlock()


def find_datasets(
    enabled=None,
    target=None,
    identifier=None,
    config="/etc/siptools_research.conf",
):
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
        search["enabled"] = enabled
    if target is not None:
        search["target"] = target
    if identifier is not None:
        search["id"] = identifier

    return list(
        Dataset(document["id"], document=document, config=config)
        for document in DatasetWorkflowEntry.objects.filter(**search)
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
