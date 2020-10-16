"""Implementation of tempfile.TemporaryDirectory for python2.7."""
import tempfile
import shutil
import os


class TemporaryDirectory(object):
    """Temporary directory context.

    Directory is created using mkdtemp, and it is removed when exiting
    context.

    This class is simplifed version of tempfile.TemporaryDirectory that
    was introduced in Python 3.2.
    """

    def __init__(self, prefix="tmp"):
        """Create temporary directory."""
        self.name = tempfile.mkdtemp(prefix=prefix)

    def __enter__(self):
        """Return path to temporary directory."""
        return self.name

    def __exit__(self, *args):
        """Remove temporary directory."""
        if os.path.exists(self.name):
            shutil.rmtree(self.name)

    def __del__(self):
        """Remove temporary directory."""
        if os.path.exists(self.name):
            shutil.rmtree(self.name)
