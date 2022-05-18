import paramiko
import luigi


class RemoteAnyTarget(luigi.Target):
    """ A list of possible file paths is given instead of one path.
    The target exists if any of those paths exist at remote host."""

    def __init__(self, path, host, port, username, keyfile):
        self.path = path
        self.host = host
        self.port = port
        self.username = username
        self.keyfile = keyfile

    def exists(self):
        """Returns ``True`` if any of paths exist."""
        for path in self.path:
            if self._exists(path):
                return True

        return False

    def existing_paths(self):
        """Returns the paths that exists."""
        existing_paths = []
        for path in self.path:
            if self._exists(path):
                existing_paths.append(path)
        return existing_paths

    def _exists(self, path):
        """Returns ``True`` if the path exists in remote host.
        :param path: path to verify at remote host
        """
        # Init SFTP connection
        exists = False
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host,
                        port=int(self.port),
                        username=self.username,
                        key_filename=self.keyfile)
            with ssh.open_sftp() as sftp:
                try:
                    sftp.stat(path)
                    exists = True
                except OSError as ex:
                    if 'No such file' not in str(ex):
                        raise
        return exists
