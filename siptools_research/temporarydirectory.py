"""Implementation of tempfile.TemporaryDirectory for python2.7."""
import tempfile
import shutil
import logging


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
        shutil.rmtree(self.name)
        self.name = None

    def __del__(self):
        """Ensure directory is removed."""
        if self.name:
            logging.warning("Cleaning %s implicitly", self.name)
            shutil.rmtree(self.name)
