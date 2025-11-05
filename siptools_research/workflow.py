"""Workflow class."""
import enum
import shutil

from metax_access import (
    DS_STATE_GENERATING_METADATA,
    DS_STATE_IN_PACKAGING_SERVICE,
    DS_STATE_INITIALIZED,
    DS_STATE_METADATA_CONFIRMED,
    DS_STATE_REJECTED_BY_USER,
    DS_STATE_VALIDATING_METADATA,
)

from siptools_research.config import Configuration
from siptools_research.dataset import Dataset
from siptools_research.exceptions import (
    AlreadyPreservedError,
    CopiedToPasDataCatalogError,
    WorkflowExistsError,
)
from siptools_research.models.workflow_entry import WorkflowEntry
from siptools_research.tasks.cleanup import Cleanup
from siptools_research.tasks.generate_metadata import GenerateMetadata
from siptools_research.tasks.report_dataset_validation_result import (
    ReportDatasetValidationResult,
)
from siptools_research.workspace import Workspace


class Target(enum.Enum):
    """Target of a workflow."""

    METADATA_GENERATION = "metadata_generation"
    VALIDATION = "validation"
    PRESERVATION = "preservation"


TARGET_TASKS = {
    Target.PRESERVATION: Cleanup,
    Target.METADATA_GENERATION: GenerateMetadata,
    Target.VALIDATION: ReportDatasetValidationResult
}


class Workflow:
    """Class for managing workflows."""

    def __init__(
        self, dataset_id, document=None, config="/etc/siptools_research.conf"
    ):
        """Initialize workflow."""
        self.config = config
        self.dataset = Dataset(dataset_id, config=config)
        configuration = Configuration(config)
        self.workspace = Workspace(
            packaging_root=configuration.get("packaging_root"),
            dataset_id=self.dataset.identifier,
        )

        self._document = document
        if self._document is None:
            try:
                self._document = WorkflowEntry.objects.get(id=dataset_id)
            except WorkflowEntry.DoesNotExist:
                self._document = WorkflowEntry(
                    id=dataset_id
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
    def enabled(self):
        """Check if workflow is enabled."""
        return self._document.enabled

    def disable(self):
        """Disable workflow."""
        self._document.enabled = False
        self._document.save()

    def enable(self):
        """Enable workflow."""
        self._document.enabled = True
        self._document.save()

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
            raise WorkflowExistsError

        if self.dataset._has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(
            DS_STATE_GENERATING_METADATA,
            "File identification started by user",
        )

        # Clear the workspaces
        for path in [
            self.workspace.metadata_generation,
            self.workspace.validation,
            self.workspace.preservation,
        ]:
            _delete_directory(path)

        self.workspace.metadata_generation.mkdir(parents=True)

        self._set_target(Target.METADATA_GENERATION)

    def validate(self):
        """Validate metadata and files of dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError

        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(
            DS_STATE_VALIDATING_METADATA,
            "Proposed for preservation by user"
        )

        # Clear the workspaces
        for path in [self.workspace.validation, self.workspace.preservation]:
            _delete_directory(path)

        self.workspace.validation.mkdir(parents=True)

        self._set_target(Target.VALIDATION)

    def preserve(self):
        """Preserve dataset.

        :returns: ``None``
        """
        if self.enabled:
            raise WorkflowExistsError

        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        # TODO: ensure that user has confirmed metadata, before dataset
        # is preserved

        self.dataset.set_preservation_state(
            DS_STATE_IN_PACKAGING_SERVICE,
            "Packaging dataset",
        )

        # Clear the preservation workspace
        _delete_directory(self.workspace.preservation)

        self.workspace.preservation.mkdir(parents=True)

        self._set_target(Target.PRESERVATION)

    def confirm(self) -> None:
        """Confirm dataset metadata."""
        # TODO: Confirmation should not be allowed if metadata has not
        # been generated.

        # Confirming metadata of dataset is pointless, if dataset is
        # already preserved
        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        # TODO: Resetting dataset or restarting metadata generation
        # should undo the confirmation.
        self.dataset.set_preservation_state(
            DS_STATE_METADATA_CONFIRMED, "Metadata confirmed by user"
        )

    def reset(self, description: str, reason_description: str) -> None:
        """Reset dataset.

        This sets its state to INITIALIZED and removes any existing file
        errors.
        """
        if self.enabled:
            raise WorkflowExistsError

        if self.dataset._has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(DS_STATE_INITIALIZED, description)
        self.dataset.set_preservation_reason(reason_description)

        # Unlock dataset
        self.dataset.unlock()

    def reject(self) -> None:
        """Reject dataset.

        Sets preservation status to DS_STATE_REJECTED_BY_USER.
        """
        if self.enabled:
            raise WorkflowExistsError

        if self.dataset._is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(DS_STATE_REJECTED_BY_USER,
                                            "Rejected by user")


def _delete_directory(path):
    """Delete directory if it exists.

    :param path: Directory path
    """
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        # Previous workspace does not exist
        pass


def find_workflows(
    enabled=None,
    target=None,
    identifier=None,
    config="/etc/siptools_research.conf",
):
    """Find workflows by search criteria.

    :param bool enabled: If `True`, show only enabled workflows, if
                         `False`, show only disabled workflows.
    :param str target: Show only workflows with this target
    :param str identifier: Show only workflows with this dataset identifier
    :param str config: Path to configuration file
    :returns: List of matching workflows
    """
    search = {}
    if enabled is not None:
        search["enabled"] = enabled
    if target is not None:
        search["target"] = target
    if identifier is not None:
        search["id"] = identifier

    return list(
        Workflow(document["id"], document=document, config=config)
        for document in WorkflowEntry.objects.filter(**search)
    )
