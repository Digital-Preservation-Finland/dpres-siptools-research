"""Workspace class."""


from pathlib import Path


class Workspace:
    """"Class managing workspace directory."""

    def __init__(self, packaging_root, dataset_id):
        """Choose path for workspace root."""
        # Currently packaging_root contains only the workspace
        # directories, but it might have other purposes in future, so
        # workspaces are created in "workspaces" subdirectory.
        self.root = Path(packaging_root) / "workspaces" / dataset_id

    @property
    def metadata_generation(self):
        """Return metadata generation workspace directory."""
        return self.root / "metadata_generation"

    @property
    def validation(self):
        """Return validation workspace directory."""
        return self.root / "validation"

    @property
    def preservation(self):
        """Return preservation workspace directory."""
        return self.root / "preservation"
