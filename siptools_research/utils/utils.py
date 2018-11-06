"""Utility functions"""

import os
import errno


def makedirs_exist_ok(path):
    """Creates directory if it does not exists already. In python 3.2
    os.makedirs has parameter ``exist_ok`` that enables same functionality.

    :param path: path to directory
    :returns: ``None``
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
