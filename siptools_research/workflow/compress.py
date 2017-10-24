"""required tasks to validate SIPs and create AIPs"""

import os
import sys
import traceback
import datetime

from luigi import Parameter

from siptools_research.workflow_x.move_sip import MoveSipToUser, FailureLog
from siptools_research.luigi.target import TaskFileTarget, MongoDBTarget
from siptools_research.utils.utils import  touch_file

from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.sign import SignSIP

from siptools.scripts.compress import main


class CompressSIP(WorkflowTask):
    """Compresses contents to a tar file as SIP.

    :path: workspace/created_sip
    """
    workspace = Parameter()
    def requires(self):
        """Requires signature file"""
        return {"Sign SIP":
                SignSIP(workspace=self.workspace,
                        sip_creation_path=self.sip_creation_path,
                        home_path=self.home_path)}

    def output(self):
       """Returns task output. Task is ready when succesful event has been
        added to worklow database.

        :returns: MongoTaskResultTarget
        """
       return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name)
    def run(self):
        """Creates a tar archive file conatining mets.xml,
        signature.sig  and all content files.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """

        sip_name = os.path.join(self.sip_output_path,
                                (os.path.basename(self.workspace) + '.tar'))

        compress_log = os.path.join(self.workspace,
                                    'logs',
                                    'task-compress-sip.log')
         # Redirect stdout to logfile
        with open(compress_log, 'w') as log:
            with redirect_stdout(log):
                try:

                    main(['--tar_filename', sip_name, self.sip_creation_path])
                    task_result = 'success'
                    task_messages = "SIP archive file compressed"



                except Exception as ex:
                   task_result = 'failure'
                   task_messages = 'Could not compress SIP, '\
                                    % exc.message

                   database.set_status(self.document_id, 'rejected')
                finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Compression of SIP "\
                                        "failed due to unknown error."

                    database.add_event(self.document_id,
                                       self.task_name,
                                       task_result,
                                       task_messages)
