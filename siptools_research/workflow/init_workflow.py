"""Process all SIPs in the workspace directory.

To run this automatically add the following lines to
/etc/cron.d/metadata::

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
    */10 * * * * * <username>  /usr/bin/metadata.py ProcessMetadata
        --workspace_root <workspaces path>
        --home-path <home path>

"""

import os
import socket

import luigi

from siptools_research.utils.utils import iter_workspaces,\
    select_items_distribute, cache_items

from siptools_research.workflow.create_digiprov \
    import CreateProvenanceInformation

from siptools_research.workflow.create_techmd import CreateTechnicalMetadata

class ProcessMetadata(luigi.WrapperTask):
    """Process all SIP workspaces in `workspace_root`.

    Calls the `ValidationWorkflow` for each SIP workspace.

    Writes the file `<workspace_root>/.state/process-ead3-<unique>`
    after completed.

    :workpace_root: Path for SIP workspaces eq.
    /var/spool/siptools_research
    :home_path: Path to user home directories eq. /home

    """

    hostname = luigi.Parameter(default=socket.gethostname())
    workspace_root = luigi.Parameter()
    home_path = luigi.Parameter()
    min_age = luigi.IntParameter()

    number_of_subtasks = luigi.IntParameter(default=10, significant=False)
    number_of_hosts = luigi.IntParameter(default=5, significant=False)
    host_number = luigi.IntParameter(default=None, significant=False)

    path_cache = []

    def requires(self):
        """Return required tasks to validate SIP and create AIP.

        :returns: None

        """

        for workspace_path in cache_items(
                select_items_distribute(
                    items=iter_workspaces(workspace_root=self.workspace_root),
                    number_of_hosts=self.number_of_hosts,
                    host_number=self.host_number
                ),
                self.path_cache, self.number_of_subtasks
        ):

            yield CreateProvenanceInformation(
                workspace=workspace_path,
                home_path=self.home_path
            )
            yield CreateTechnicalMetadata(
                workspace=workspace_path,
                sip_creation_path=os.path.join(workspace_path,
                                               'sip-in-progress'),
                home_path=self.home_path)


if __name__ == '__main__':
    luigi.run()
