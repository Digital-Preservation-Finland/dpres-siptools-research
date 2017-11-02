"""A workflow task that reads the ingest report locations from preservation
service and updates preservation status to Metax."""

from ..luigi.task import WorkflowTask
from ..luigi.target import MongoTaskResultTarget
from .validate_sip import ValidateSIP
from ..utils import database

class ReportPreservationStatus(WorkflowTask):
    """A luigi task that copies and reads the ingest report from preservation
    service. The preservation status is updated to Metax."""

    def requires(self):
        return ValidateSIP(workspace=self.workspace,
                           dataset_id=self.dataset_id)

    def output(self):
        return MongoTaskResultTarget(self.document_id, self.task_name)

    def run(self):

        # List of all matching paths ValidateSIP found
        ingest_report_paths = self.input().existing_paths()

        # Only one ingest report should be found
        assert len(ingest_report_paths) == 1

        # 'accepted' or 'rejected'?
        directory = ingest_report_paths[0].split('/')[0]
        print directory
        if directory == 'accepted':
            print 'ACCEPTED'
        elif directory == 'rejected':
            print 'REJECTED'
        else:
            raise  ValueError('Ingest report was found incorrect path: %s'\
                              % ingest_report_paths[0])

        task_result = "success"
        task_messages = "Accepted to preservation"
        database.add_event(self.document_id,
                           self.task_name,
                           task_result,
                           task_messages)
