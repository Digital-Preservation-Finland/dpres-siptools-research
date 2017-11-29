"""Luigi task that signs METS file"""

import os
import luigi
from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_mets import CreateMets
from siptools_research.utils import contextmanager
import siptools_research.utils.database
from siptools.scripts import sign_mets

SIGN_KEY_PATH = '/home/vagrant/sip_sign_pas.pem'

class SignSIP(WorkflowTask):
    """Task that signs METS file.
    """
    sign_key_path = luigi.Parameter(default=SIGN_KEY_PATH)

    def requires(self):
        """Returns required tasks.

        :returns: CreateMets task
        """
        return CreateMets(workspace=self.workspace, dataset_id=self.dataset_id)

    def output(self):
        """Returns task output targets.

        :returns: MongoTaskResultTarget
        """
        return MongoTaskResultTarget(self.document_id, self.task_name,
                                     self.config)

    def run(self):
        """Signs METS file and adds event to workflow database.

        :returns: None
        """
        # mets_location = os.path.join(self.sip_creation_path, 'mets.xml')
        mets_location = 'mets.xml'
        signature_file = os.path.join(self.sip_creation_path, 'signature.sig')

        log_path = os.path.join(self.workspace, 'logs', 'task-sign-sip.log')
        with open(log_path, 'w') as log:
            with contextmanager.redirect_stdout(log):
                sign_mets.main([mets_location,
                                signature_file,
                                self.sign_key_path])

        task_result = "success"
        task_messages = "Digital signature for SIP created."
        database = siptools_research.utils.database.Database(self.config)
        database.add_event(self.document_id,
                           self.task_name,
                           task_result,
                           task_messages)
