"""Workflow class."""
import contextlib
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
    MetadataNotConfirmedError,
    MetadataNotGeneratedError,
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
    """Class for managing workflows.

    Manages workflows related to datasets, and decides which actions are
    allowed based on the current status of the workflow.
    """

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

    @property
    def metadata_confirmed(self):
        """Whether dataset metadata has been confirmed."""
        return self._document.metadata_confirmed

    def _set_target(self, target):
        """Set target of workflow.

        param Target target: The target of the workflow
        """
        self._document.target = target.value
        self._document.enabled = True
        self._document.save()

    @property
    def active(self):
        """Check if workflow is active.

        The workflow is active if it is enabled and the target task is
        not complete.
        """
        if not self.enabled:
            return False

        target_task = TARGET_TASKS[self.target](
            dataset_id=self.dataset.identifier,
            config=self.config,
        )

        return not target_task.complete()

    def generate_metadata(self):
        """Initialize metadata generation workflow."""
        if self.active:
            raise WorkflowExistsError

        if self.dataset.has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        self.dataset.lock()

        self._document.metadata_confirmed = False

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
        """Initialize dataset validation workflow."""
        if self.active:
            raise WorkflowExistsError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        # Metadata does not necessarily has to be confirmed when
        # validation is started, but in practise the validation workflow
        # is initialized when user proposes the dataset for
        # preservation, and therefore metadta must be confirmed when the
        # dataset generated. Anyway, at least the metadata has to be
        # generated before it is validated.
        if not self.metadata_confirmed:
            raise MetadataNotConfirmedError

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
        """Initialize preservation workflow."""
        if self.active:
            raise WorkflowExistsError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        if not self.metadata_confirmed:
            raise MetadataNotConfirmedError

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
        # Dataset metadata can not be confirmed if has not even been
        # generated yet
        metadata_generation_task = TARGET_TASKS[Target.METADATA_GENERATION](
            dataset_id=self.dataset.identifier,
            config=self.config,
        )
        if not metadata_generation_task.complete():
            raise MetadataNotGeneratedError

        # Confirming metadata of dataset is pointless, if dataset is
        # already preserved
        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        self._document.metadata_confirmed = True
        self._document.save()

        self.dataset.set_preservation_state(
            DS_STATE_METADATA_CONFIRMED, "Metadata confirmed by user"
        )

    def reset(self, description: str, reason_description: str) -> None:
        """Reset dataset.

        This sets its state to INITIALIZED and removes any existing file
        errors.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(DS_STATE_INITIALIZED, description)
        self.dataset.set_preservation_reason(reason_description)

        # Unlock dataset
        self.dataset.unlock()

        # Because the metadata might change, user has to confirm the
        # metadata again.
        self._document.metadata_confirmed = False
        self._document.save()

    def reject(self) -> None:
        """Reject dataset.

        Sets preservation status to DS_STATE_REJECTED_BY_USER.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        self.dataset.set_preservation_state(DS_STATE_REJECTED_BY_USER,
                                            "Rejected by user")


def _delete_directory(path):
    """Delete directory if it exists.

    :param path: Directory path
    """
    with contextlib.suppress(FileNotFoundError):
        shutil.rmtree(path)


def find_workflows(
    enabled=None,
    identifier=None,
    config="/etc/siptools_research.conf",
):
    """Find workflows by search criteria.

    :param bool enabled: If `True`, show only enabled workflows, if
                         `False`, show only disabled workflows.
    :param str identifier: Show only workflows with this dataset identifier
    :param str config: Path to configuration file
    :returns: List of matching workflows
    """
    search = {}
    if enabled is not None:
        search["enabled"] = enabled
    if identifier is not None:
        search["id"] = identifier

    return [
        Workflow(document["id"], document=document, config=config)
        for document in WorkflowEntry.objects.filter(**search)
    ]
