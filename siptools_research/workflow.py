"""Workflow class."""
from __future__ import annotations

import contextlib
import shutil
from typing import TYPE_CHECKING

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
    AlreadyProposedError,
    AlreadyPreservedError,
    CopiedToPasDataCatalogError,
    DatasetHasErrorsError,
    MetadataAlreadyConfirmedError,
    MetadataNotConfirmedError,
    MetadataNotGeneratedError,
    NotProposedError,
    WorkflowExistsError,
)
from siptools_research.models.workflow_entry import WorkflowEntry
from siptools_research.tasks.cleanup import Cleanup
from siptools_research.tasks.generate_metadata import GenerateMetadata
from siptools_research.tasks.report_dataset_validation_result import (
    ReportDatasetValidationResult,
)
from siptools_research.workspace import Workspace

if TYPE_CHECKING:
    from pathlib import Path


TARGET_TASKS = {
    "preservation": Cleanup,
    "metadata_generation": GenerateMetadata,
    "validation": ReportDatasetValidationResult,
}


class Workflow:
    """Class for managing workflows.

    Manages workflows related to datasets, and decides which actions are
    allowed based on the current status of the workflow.
    """

    def __init__(
        self,
        dataset_id: str,
        document: WorkflowEntry | None = None,
        config: str = "/etc/siptools_research.conf",
    ) -> None:
        """Initialize workflow instance.

        :param dataset_id: Identifier of the dataset
        :param document: Optional workflow status document
        :param config: Path to the configuration file
        """
        self.config = config
        self.dataset = Dataset(dataset_id, config=config)
        configuration = Configuration(config)
        self.workspace = Workspace(
            packaging_root=configuration.get("packaging_root"),
            dataset_id=self.dataset.identifier,
        )

        if document:
            self._document = document
        else:
            try:
                self._document = WorkflowEntry.objects.get(id=dataset_id)
            except WorkflowEntry.DoesNotExist:
                self._document = WorkflowEntry(
                    id=dataset_id
                )

    @property
    def target(self) -> str | None:
        """Target of the workflow."""
        return self._document.target

    @property
    def paused(self) -> bool:
        """Check if workflow is paused."""
        return self._document.paused

    def pause(self) -> None:
        """Pause workflow."""
        self._document.paused = True
        self._document.save()

    def resume(self) -> None:
        """Resume paused workflow."""
        self._document.paused = False
        self._document.save()

    @property
    def metadata_confirmed(self) -> bool:
        """Whether dataset metadata has been confirmed."""
        return self._document.metadata_confirmed

    @property
    def proposed(self) -> bool:
        """Whether dataset has been proposed for preservation."""
        return self._document.proposed

    def _set_target(self, target: str) -> None:
        """Set target of workflow.

        :param target: The target of the workflow
        """
        self._document.target = target
        self._document.save()

    @property
    def active(self) -> bool:
        """Check if workflow is active.

        The workflow is active if errors have not been found from
        dataset, and the target task is not complete.
        """
        if not self.target:
            return False

        if self.dataset.errors:
            return False

        target_task = TARGET_TASKS[self.target](
            dataset_id=self.dataset.identifier,
            config=self.config,
        )

        return not target_task.complete()

    def require_generate_metadata_allowed(self) -> None:
        """Ensure that metadata generation is allowed.

        :raises ActionNotAllowedError: if metadata generation is not
            allowed for some reason.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

    def generate_metadata(self) -> None:
        """Initialize metadata generation workflow."""
        self.require_generate_metadata_allowed()

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

        self._set_target("metadata_generation")

    def require_preserve_allowed(self) -> None:
        """Ensure that preservation is allowed.

        :raises ActionNotAllowedError: If preservation is not allowed
            for some reason.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        if not self.metadata_confirmed:
            raise MetadataNotConfirmedError

        if self.dataset.errors:
            raise DatasetHasErrorsError

    def preserve(self) -> None:
        """Initialize preservation workflow."""
        self.require_preserve_allowed()

        self.dataset.set_preservation_state(
            DS_STATE_IN_PACKAGING_SERVICE,
            "Packaging dataset",
        )

        # Clear the preservation workspace
        _delete_directory(self.workspace.preservation)

        self.workspace.preservation.mkdir(parents=True)

        self._set_target("preservation")

    def require_confirm_allowed(self) -> None:
        """Ensure that confirming metadata is allowed.

        :raises ActionNotAllowedError: If confirming metadata is not
            allowed for some reason.
        """
        if self.active:
            raise WorkflowExistsError

        # Confirming metadata of dataset is pointless, if dataset is
        # already preserved
        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        # Dataset metadata can not be confirmed if has not even been
        # generated yet
        metadata_generation_task = TARGET_TASKS["metadata_generation"](
            dataset_id=self.dataset.identifier,
            config=self.config,
        )
        if not metadata_generation_task.complete():
            raise MetadataNotGeneratedError

        if self.metadata_confirmed:
            raise MetadataAlreadyConfirmedError

        if self.dataset.errors:
            raise DatasetHasErrorsError

    def confirm(self) -> None:
        """Confirm dataset metadata."""
        self.require_confirm_allowed()

        self._document.metadata_confirmed = True
        self._document.save()

        self.dataset.set_preservation_state(
            DS_STATE_METADATA_CONFIRMED, "Metadata confirmed by user"
        )

    def require_reset_allowed(self) -> None:
        """Ensure that reset is allowed.

        :raises ActionNotAllowedError: If reset is not allowed for some
            reason.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

    def reset(self, description: str, reason_description: str) -> None:
        """Reset dataset.

        Sets dataset state to INITIALIZED, unlocks the dataset, and
        resets workflow state.
        """
        self.require_reset_allowed()

        self.dataset.set_preservation_state(DS_STATE_INITIALIZED, description)
        self.dataset.set_preservation_reason(reason_description)

        self._document.proposed = False
        self._document.save()

        # Unlock dataset
        self.dataset.unlock()

        # Delete workflow document from database
        self._document.delete()

        # Reset state of the current workflow object
        self._document = WorkflowEntry(id=self.dataset.identifier)

    def require_propose_allowed(self) -> None:
        """Ensure that proposing dataset for preservation is allowed.

        :raises ActionNotAllowedError: If propsing is not allowed for
            some reason.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        if not self.metadata_confirmed:
            raise MetadataNotConfirmedError

        if self.proposed:
            raise AlreadyProposedError

        if self.dataset.errors:
            raise DatasetHasErrorsError

    def propose(self) -> None:
        """Propose dataset for preservation.

        Proposing dataset for preservation will automatically start
        validation workflow.
        """
        self.require_propose_allowed()

        self._document.proposed = True
        self._document.save()

        self.dataset.set_preservation_state(
            DS_STATE_VALIDATING_METADATA,
            "Proposed for preservation by user"
        )

        # Clear the workspaces
        # NOTE: Mostly for historical reasons (see TPASPKT-998), the
        # dataset is always validated when the user proposes dataset for
        # preservation. In some cases it might be unnecessary, for
        # example if the proposal is rejected, and then dataset is
        # proposed again without making any changes to the dataset. So
        # maybe we could check if the dataset has already been
        # validated when it is proposed for preservation, and implement
        # a specific endpoint that user can use to restart validation
        # when needed?
        for path in [self.workspace.validation, self.workspace.preservation]:
            _delete_directory(path)

        self.workspace.validation.mkdir(parents=True)

        self._set_target("validation")

    def require_reject_allowed(self) -> None:
        """Ensure that rejecting proposal is allowed.

        :raises ActionNotAllowedError: If rejection is not allowed for
            some reason.
        """
        if self.active:
            raise WorkflowExistsError

        if self.dataset.has_been_copied_to_pas_datacatalog:
            raise CopiedToPasDataCatalogError

        if self.dataset.is_preserved:
            raise AlreadyPreservedError

        if not self.proposed:
            raise NotProposedError

    def reject(self) -> None:
        """Reject proposal for digital preservation.

        Sets preservation status to DS_STATE_REJECTED_BY_USER.
        """
        self.require_reject_allowed()

        self._document.proposed = False
        self._document.save()

        self.dataset.set_preservation_state(DS_STATE_REJECTED_BY_USER,
                                            "Rejected by user")


def _delete_directory(path: Path) -> None:
    """Delete directory if it exists.

    :param path: Directory path
    """
    with contextlib.suppress(FileNotFoundError):
        shutil.rmtree(path)


def find_workflows(
    *,
    active: bool | None = None,
    paused: bool | None = None,
    identifier: str | None = None,
    config: str = "/etc/siptools_research.conf",
) -> list[Workflow]:
    """Find workflows by search criteria.

    :param active: If `True`, show only active workflows
    :param paused: If `True`, show only paused workflows
    :param identifier: Show only workflows with this dataset identifier
    :param config: Path to configuration file
    """
    search = {}
    if paused:
        search["paused"] = True
    if identifier is not None:
        search["id"] = identifier

    filtered_workflows = [
        Workflow(document["id"], document=document, config=config)
        for document
        in WorkflowEntry.objects.filter(**search)
    ]

    if active:
        workflows = [
            workflow
            for workflow in filtered_workflows
            if workflow.active
        ]
    else:
        workflows = filtered_workflows

    return workflows
