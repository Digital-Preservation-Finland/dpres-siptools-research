"""Search for files in the users' home/transfers directories and
call MoveSipToWorkspace on them.

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
import logging
import itertools

import luigi

from siptools_research.utils import select_items_distribute, cache_items

from siptools_research.workflow.utils import iter_files

from siptools_research.transfer.transfer import MoveTransferToWorkspace


LOGGER = logging.getLogger('siptools_research.scripts.process_transfers')



def get_transfers(home_path):
    """A generator, which yields the transfers under
       <home_path>/<username>/transfer

    :home_path: The path where the users' home directories are
    :returns: yields a tuple of (username, path)

    """

    for username in os.listdir(home_path):
        transfer_path = os.path.join(home_path, username, 'transfer')

        if not os.path.isdir(transfer_path):
            continue

        # NOTE: Why don't we just write:
        #   for filename in listdir(transfer_path):
        #       filepath = os.path.join(path, filename)
        #       yield {'username': username, 'path': filepath}

        transfer_iter = iter_files(transfer_path)

        for path in itertools.chain(transfer_iter):
            yield {'username': username, 'path': path}


class ProcessTransfers(luigi.WrapperTask):
    """Search for files in the users' home directories and
    call MoveSipToWorkspace on them.

    :workspace_root: Path for workspaces (eq. /var/spool/siptools_research)
    :home_path: Path to user home directories (eq. /home)
    :min_age: Minimum st_ctime for transferred file before processing
    """

    workspace_root = luigi.Parameter()
    home_path = luigi.Parameter()
    min_age = luigi.Parameter()

    number_of_subtasks = luigi.IntParameter(default=10, significant=False)
    number_of_hosts = luigi.IntParameter(default=5, significant=False)
    host_number = luigi.IntParameter(default=None, significant=False)

    path_cache = []

    def requires(self):
        """The actual run method. Loops through the transfers and, if they are
        older than min_age seconds, calls MoveTransferToWorkspace
        """

        for dataset_file in cache_items(
                select_items_distribute(
                    get_transfers(self.home_path),
                    number_of_hosts=self.number_of_hosts,
                    host_number=self.host_number),
                self.path_cache, self.number_of_subtasks):

            LOGGER.info("Transfer file to processing %s", dataset_file)

            yield MoveTransferToWorkspace(
                filename=dataset_file["path"],
                workspace_root=self.workspace_root,
                min_age=self.min_age,
                username=dataset_file["username"])


if __name__ == '__main__':
    luigi.run()
