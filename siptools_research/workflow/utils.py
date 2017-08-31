"""Utility functions for the Luigi tasks"""

import os
import time

import scandir


def iter_files(path):
    """TODO: Docstring for iter_files.

    :paths: TODO
    :returns: TODO

    """
    if isinstance(path, list):
        path = reduce(os.path.join, path)
    for filename in os.listdir(path):
        yield os.path.join(path, filename)


def _get_newest_item_in_dir(path):
    """Return the path to the newest file in the directory
    tree pointed to by the 'path' parameter and the ctime of that file as a
    tuple. Often the newest item ca also be a directory entry."""
    paths = set()
    for root, dirs, files in scandir.walk(path, topdown=False):
        filelist = [os.path.join(root, f) for f in files]
        dirlist = [os.path.join(root, d) for d in dirs]
        paths = paths.union(filelist + dirlist + [root])

    # Keep the paths sorted so that the function output will be more
    # predictable for testability. Often the ctime of the newest file and the
    # directory entry are the same. Reverse sorting the paths means files are
    # preferred to directories when the ctime is the same.
    paths_sorted = sorted(paths, reverse=True)
    ctime_max = 0
    fname_max = None
    for item in paths_sorted:
        ctime_i = os.path.getctime(item)
        if ctime_i > ctime_max:
            ctime_max = ctime_i
            fname_max = item
    return (fname_max, ctime_max)


def iter_transfers(workspace):
    """Iterate all Transfers extracted under the `<workspace>/transfers/`
    path.

    :workspace: Workspace directory path
    :returns: Iterator for all Transfer paths

    """
    return iter_files([workspace, 'transfers'])


def iter_sips(workspace):
    """Iterate all SIPs in progress under the
    `<workspace>/sip-in-progress/` path.

    :workspace: Workspace directory path
    :returns: Iterator for all SIP paths

    """
    sip_path = os.path.join(workspace, 'sip-in-progress')
    yield sip_path


class UnknownReturnCode(Exception):
    """Raised when external process returns unknown returncode"""
    pass


def file_age(path):
    """Return file age from st_ctime for given `path`

    :path: Path to file or directory
    :returns: Age as float
    """

    if os.path.isdir(path):
        (_, ctime) = _get_newest_item_in_dir(path)
    else:
        ctime = os.path.getctime(path)
    return time.time() - ctime
