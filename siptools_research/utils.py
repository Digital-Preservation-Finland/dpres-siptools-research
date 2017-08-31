"""Utilities"""

import os
import random
from itertools import islice
import datetime
import time

import scandir

class Directory(str):
    """Automatically create directory structures"""

    def __new__(cls, path):
        """Str immutable types call __new__() to instantiate new classes.

        Create directory when directory class is created"""

        if not os.path.isdir(path):
            os.makedirs(path)

        return str.__new__(cls, path)

    def subdir(self, directory):
        """Return Directory object to subdirectory `<self>/<directory>

        :directory: Subdirectory name
        :returns: Directory object to subdirectory

        """
        return Directory(os.path.join(self, directory))

    def __getattr__(self, attr):
        """Return original class attribute ethods or self.subdir(attr) if
        attribute does not exist.

        :attr: Attribute
        :returns: Attribute or Directory object

        """

        try:
            if attr in self.__dict__:
                return self.__dict__[attr]
            return self.subdir(attr)
        except Exception as exception:
            raise AttributeError(str(exception))


def touch_file(output_target):
    """Create empty file to output target

    :outfile: Luigi LocalTarget instance
    :returns: None

    """
    with output_target.open('w') as outfile:
        outfile.write('')


def iter_workspaces(workspace_root):

    """Iterate all workspaces under given `workspace_root`.

    This will return all workspaces directly `workspace_root`/<workspace name>
    or under `workspace_root`/<batch name>/<workspace name>.

    :workspace_root: Path to search workspaces from
    :returns: Iterable of workspace paths

    """

    for batch in scandir.scandir(workspace_root):
        # iterate workspaces directly under workspace_root
        if is_workspace(batch.path):
            yield batch.path
        else:
            # iterate workspaces under batch directories
            for workspace in scandir.scandir(batch.path):
                if is_workspace(workspace.path):
                    yield workspace.path


def is_workspace(path):
    """Return True if given path is a workspace.

    :path: Absolute path to workspace
    :returns: Boolean

    """
    return os.path.isdir(os.path.join(path, 'transfers'))


def cache_items(items, item_cache, number_of_items=None):
    """Cache `number_of_items` from `items` in `item_cache` list.

    This function provides trivial cache which returns 1) always first iterated
    items even if items-iterable would change it's results 2) fast even if
    items-iterable yield results slowly.

    :items: Iterable of items to cache
    :item_cache: Empty list to be used as cache
    :number_of_items: Number of items to cache
    :returns: List of items

    """

    if item_cache:
        return item_cache

    if not isinstance(item_cache, list):
        raise Exception('Must provide empty list as item cache')

    for item in islice(items, 0, number_of_items):
        item_cache.append(item)

    return item_cache


def select_items_distribute(items, number_of_hosts, host_number=None):

    """Select `items` for `host_number` distributed over `number_of_hosts`.

    If host_number is omitted (or None) a random host number is assigned.

    :items: Iterable of items to select from
    :number_of_hosts: Number of hosts to distribute over
    :host_number: Host number or None
    :returns: Selected items

    """
    if host_number is None:
        host_number = random.randint(0, number_of_hosts - 1)

    queue = islice(
        items, host_number, None, number_of_hosts)

    for workspace_path in queue:
        yield workspace_path


def date_str():
    """Return current date as string 'YYYY-MM-DD'.

    :returns: Date as string

    """
    date = datetime.datetime.fromtimestamp(time.time())
    return date.strftime('%Y-%m-%d')
