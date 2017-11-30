"""A workflow task that reads the ingest report locations from preservation
service and updates preservation status to Metax."""

from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.workflow.validate_sip import ValidateSIP
import siptools_research.utils.database
from siptools_research.utils import metax

class ReportPreservationStatus(WorkflowTask):
    """A luigi task that copies and reads the ingest report from preservation
    service. The preservation status is updated to Metax."""

    def requires(self):
        return ValidateSIP(workspace=self.workspace,
                           dataset_id=self.dataset_id)

    def output(self):
        return MongoTaskResultTarget(self.document_id, self.task_name,
                                     self.config)

    def run(self):

        # List of all matching paths ValidateSIP found
        ingest_report_paths = self.input().existing_paths()

        # Only one ingest report should be found
        assert len(ingest_report_paths) == 1

        # 'accepted' or 'rejected'?
        metax_client = metax.Metax(self.config)
        directory = ingest_report_paths[0].split('/')[0]
        if directory == 'accepted':
            # Set Metax preservation state of this dataset to 6 ("in longterm
            # preservation")
            metax_client.set_preservation_state(self.dataset_id, '6')
        elif directory == 'rejected':
            # Set Metax preservation state of this dataset  to 7  ("Rejected
            # long-term preservation")
            metax_client.set_preservation_state(self.dataset_id, '7')
        else:
            raise  ValueError('Ingest report was found in incorrect path: %s'\
                              % ingest_report_paths[0])

        task_result = "success"
        task_messages = "Accepted to preservation"
        database = siptools_research.utils.database.Database(self.config)
        database.add_event(self.document_id,
                           self.task_name,
                           task_result,
                           task_messages)
