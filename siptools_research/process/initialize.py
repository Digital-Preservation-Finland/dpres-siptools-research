"""Search for SIPs (transfers) in the users' home directories and call
MoveSipToWorkspace on them.

To run this automatically add the following lines to
/etc/cron.d/initialize::

    SHELL=/bin/bash
    PATH=/sbin:/bin:/usr/sbin:/usr/bin
    MAILTO=root
    HOME=/
    # .---------------- minute (0 - 59)
    # | .------------- hour (0 - 23)
    # | | .---------- day of month (1 - 31)
    # | | | .------- month (1 - 12) OR jan,feb,mar,apr ...
    # | | | | .---- day of week (0 - 6) (Sunday=0 or 7) OR
    # sun,mon,tue,wed,thu,fri,sat
    # | | | | |
    */10 * * * * * <username>  /usr/bin/initialize.py ProcessTransfers
        --workspace-root <workspaces path>
        --home-path <home path>
        --min-age <the minimum age for the files which will be processed>

"""

import os
import socket
import logging
import itertools

import luigi

from siptools_research.utils import select_items_distribute, cache_items

from siptools_research.workflow.utils import iter_files

from siptools_research.transfer.transfer import MoveTransferToWorkspace


LOGGER = logging.getLogger('siptools_research.scripts.process_transfers')


def touch_file(output_target):
    """Create empty file to output target

    :outfile: Luigi LocalTarget instance
    :returns: None

    """
    with output_target.open('w') as outfile:
        outfile.write('')


def get_transfers(home_path):
    """A generator, which yields the transfers under
       <home_path>/<username>/transfer and
       <home_path>/<username>/approved

    :home_path: The path where the users' home directories are
    :returns: yields a tuple of (username, path)

    """

    for username in os.listdir(home_path):
        transfer_path = os.path.join(home_path, username, 'transfer')

        if not os.path.isdir(transfer_path):
            continue

        transfer_iter = iter_files(transfer_path)

        for path in itertools.chain(transfer_iter):
            if not is_sip(path):
                continue
            yield {'username': username, 'path': path}


def is_sip(path):
    """A path is believed to be a SIP if it's a directory with a
    sahke2.xml file in it or if it's a file with the .tar or .tar.gz
    suffix

    :path: The path to the file or directory
    :returns: boolean
    """
    if os.path.isdir(path):
        if os.path.exists(os.path.join(path, 'sahke2.xml')):
            return True
    if os.path.isfile(path) and (path.endswith('.tar.gz') or
                                 path.endswith('.tar')):
        return True
    return False


class ProcessTransfers(luigi.WrapperTask):
    """Search for SIPs (transfers) in the users' home directories and
    call MoveSipToWorkspace on them.

    :workspace_root: Path for SIP workspaces
    (eq. /var/spool/siptools_research)
    :home_path: Path to user home directories (eq. /home)
    :min_age: Minimum st_ctime for transferred SIP before processing

    """

    workspace_root = luigi.Parameter()
    home_path = luigi.Parameter()
    min_age = luigi.Parameter()
    hostname = luigi.Parameter(default=socket.gethostname())

    number_of_subtasks = luigi.IntParameter(default=10, significant=False)
    number_of_hosts = luigi.IntParameter(default=5, significant=False)
    host_number = luigi.IntParameter(default=None, significant=False)

    path_cache = []

    def requires(self):
        """The actual run method. Loops through the transfers and, if
        they are older than min_age seconds, calls
        MoveTransferToWorkspace

        """

        for sip in cache_items(
                select_items_distribute(
                    get_transfers(self.home_path),
                    number_of_hosts=self.number_of_hosts,
                    host_number=self.host_number),
                self.path_cache, self.number_of_subtasks):

            LOGGER.info("Transfer SIP to processing %s", sip)

            yield MoveTransferToWorkspace(
                filename=sip["path"],
                workspace_root=self.workspace_root,
                min_age=self.min_age,
                home_path=self.home_path,
                username=sip["username"])


if __name__ == '__main__':
    luigi.run()
