import os

import mockssh.server
import mockssh.sftp
import paramiko


class HomeDirSFTPServerInterface(mockssh.sftp.SFTPServerInterface):
    """
    Subclass of mockssh.sftp.SFTPServerInterface that places the user in the
    given directory, instead of defaulting to the current working directory
    """
    def __init__(self, server, *largs, **kwargs):
        self.server = server

    @property
    def home_dir(self):
        """
        Path to the home directory
        """
        return self.server.server.home_dir

    def path_to_abs(self, path):
        return os.path.join(self.home_dir, path)

    def open(self, path, flags, attr):
        path = self.path_to_abs(path)
        return super().open(path, flags, attr)

    def stat(self, path):
        path = self.path_to_abs(path)
        return super().stat(path)

    def lstat(self, path):
        path = self.path_to_abs(path)
        return super().lstat(path)

    def symlink(self, src, dest):
        src = self.path_to_abs(src)
        dest = self.path_to_abs(dest)
        return super().symlink(src, dest)

    def remove(self, path):
        path = self.path_to_abs(path)
        return super().remove(path)

    def mkdir(self, path, attrs):
        path = self.path_to_abs(path)
        return super().mkdir(path, attrs)

    def rmdir(self, path):
        path = self.path_to_abs(path)
        return super().rmdir(path)

    def chattr(self, path, attrs):
        path = self.path_to_abs(path)
        return super().chattr(path, attrs)

    def rename(self, src, dest):
        src = self.path_to_abs(src)
        dest = self.path_to_abs(dest)
        return super().rename(src, dest)

    def list_folder(self, path):
        path = self.path_to_abs(path)
        return super().list_folder(path)


class HomeDirSFTPServer(paramiko.SFTPServer):
    """
    Subclass of paramiko.SFTPServer that places the user in the given directory
    instead of using the current working directory
    """
    def __init__(self, *args, **kwargs):
        kwargs["sftp_si"] = HomeDirSFTPServerInterface
        super().__init__(*args, **kwargs)


class HomeDirMockServer(mockssh.server.Server):
    """
    Subclass of mockssh.Server that emulates a SFTP server that places
    the user into the given home directory
    """
    def __init__(self, *args, **kwargs):
        self.home_dir = kwargs.pop("home_dir")

        super().__init__(*args, **kwargs)
