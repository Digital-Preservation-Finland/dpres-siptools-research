"""Workspace class."""


from pathlib import Path


class Workspace:
    """"Class managing workspace directory."""

    def __init__(
        self,
        packaging_root: str,
        dataset_id: str,
    ) -> None:
        """Choose path for workspace root.

        :param packaging_root: Root directory of packaging service
        :param dataset_id: Identifier of dataset
        """
        # Currently packaging_root contains only the workspace
        # directories, but it might have other purposes in future, so
        # workspaces are created in "workspaces" subdirectory.
        self.root: Path = Path(packaging_root) / "workspaces" / dataset_id

    @property
    def metadata_generation(self) -> Path:
        """Return metadata generation workspace directory."""
        return self.root / "metadata_generation"

    @property
    def validation(self) -> Path:
        """Return validation workspace directory."""
        return self.root / "validation"

    @property
    def preservation(self) -> Path:
        """Return preservation workspace directory."""
        return self.root / "preservation"
