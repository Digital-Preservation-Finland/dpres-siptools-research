"""Luigi targets"""

import luigi.contrib.ssh


class RemoteAnyTarget(luigi.contrib.ssh.RemoteTarget):
    """Modified version of luigi.contrib.ssh.RemoteTarget. A list of possible
    file paths is given instead of one path. The target exists if any of those
    paths exist at remote server."""
    def exists(self):
        """Returns ``True`` if any of paths exist."""
        return any([self.fs.exists(p) for p in self.path])

    def existing_paths(self):
        """Returns the paths that exists."""
        existing_paths = []
        for path in self.path:
            if self.fs.exists(path):
                existing_paths.append(path)
        return existing_paths
