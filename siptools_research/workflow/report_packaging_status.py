"""Task that reports preservation status after SIP creation."""

import os
from luigi import LocalTarget
from metax_access import Metax, DS_STATE_VALID_METADATA
from siptools_research.workflowtask import WorkflowTask
from siptools_research.workflow.compress import CompressSIP
from siptools_research.config import Configuration


class ReportPackagingStatus(WorkflowTask):
    """Task that sets preservation status when SIP has been created.

    A false target `report-packaging-status.finished` is created into
    workspace directory to notify luigi (and dependent tasks) that this
    task has finished.

    The task requires SIP to be created.
    """

    success_message = "Preservation status reported to Metax"
    failure_message = "Preservation status could not be set"

    def requires(self):
        """List the Tasks that this Task depends on.

        :returns: CompressSIP task
        """
        return CompressSIP(workspace=self.workspace,
                           dataset_id=self.dataset_id,
                           config=self.config)

    def output(self):
        """Return the output targets of this Task.

        :returns: `<workspace>/report-packaging-status.finished`
        :rtype: LocalTarget
        """
        return LocalTarget(
            os.path.join(
                self.workspace, 'report-packaging-status.finished'
            )
        )

    def run(self):
        """Report preservation status to Metax.

        :returns: ``None``
        """
        # Init metax
        config = Configuration(self.config)
        metax_client = Metax(
            config.get('metax_url'),
            config.get('metax_user'),
            config.get('metax_password'),
            verify=config.getboolean('metax_ssl_verification')
        )
        # TODO: There is no preservation state that would indicate
        # that SIP has been created, and currently there is no other
        # way to communicate with admin-rest-api. Therefore, the
        # preservation state is set to DS_STATE_VALID_METADATA.
        metax_client.set_preservation_state(
            self.dataset_id,
            DS_STATE_VALID_METADATA,
            'SIP created'
        )
        with self.output().open('w') as output:
            output.write('Dataset id=' + self.dataset_id)
