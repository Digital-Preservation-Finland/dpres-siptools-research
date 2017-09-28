"""Task classes for the transferred submission information package
"""

import os
import datetime

from uuid import uuid4

from luigi import Parameter, IntParameter, Task, ExternalTask, LocalTarget

from siptools_research.target import MongoDBTarget

from siptools_research.workflow.utils import file_age


class MoveTransferToWorkspace(Task):
    """Move transferred package from users home directory to unique
    workspace directory.

    :workspace: Unique workspace path (inherit from WorkflowTask)
    :unique: Unique SIP UUID, most usually basename(workspace)
    :username: Username for the transfer
    :filename: Source transfer filename

    Creates a document in mongoDB for the transfer, used for the
    reporting tool, with the following strucure:

    "_id" : [SIP id, name of workspace],
    "transfer_name" : [Transfer file name],
    "timestamp" : [Database document status timestamp],
    "username" : [Username],
    "status" : [status of the transfer/sip],
    "wf_tasks" : {[All workflow tasks are appended here, with
                   outcome and timestamp]}

    Sets the status to 'transfer received'.

    """

    filename = Parameter()
    workspace_root = Parameter()
    min_age = IntParameter()
    username = Parameter()

    def requires(self):
        """Wait until the transfer is at least self.min_age old
        (ReadyForTransfer completes at that point).
        """
        return ReadyForTransfer(
            filename=self.filename,
            min_age=self.min_age)

    def complete(self):
        """When the transfer does not exist anymore, this task is
        complete
        """
        return not os.path.exists(self.filename)

    def run(self):
        """Run the task - Move transfer and create database document.

        :returns: None

        """
        unique = str(uuid4())
        username = self.username

        # get basename without externsions
        transfer_name = os.path.basename(self.filename)

        if transfer_name[-7:] == '.tar.gz':
            document_name = transfer_name[:-7]
        elif transfer_name[-4:] == '.tar':
            document_name = transfer_name[:-4]
        else:
            document_name = transfer_name

        document_id = '%s-%s' % (document_name, unique)

        workspace = os.path.join(self.workspace_root, document_id)
        transfers_path = os.path.join(workspace, 'transfers')

        if not os.path.isdir(transfers_path):
            os.makedirs(transfers_path)

        # Move transfer
        os.rename(
            self.filename,
            os.path.join(
                transfers_path,
                os.path.basename(self.filename)))

        # Create mongoDB document
        mongo_status = 'transfer received'
        mongo_task = {
            'result': 'success',
            'messages': 'Transfer moved to workspace.',
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        create_mongodb_document(document_id, username, mongo_status,
                                transfer_name, mongo_task)


class ReadyForTransfer(ExternalTask):
    """Check that the file is older than min_age."""
    filename = Parameter()
    min_age = IntParameter()

    def output(self):
        return LocalTarget(self.filename)

    def complete(self):
        # Even though min_age is an IntParameter, it still has to be converted
        # to int.
        self.min_age = int(self.min_age)
        return file_age(self.filename) > self.min_age


def create_mongodb_document(document_id, username, mongo_status,
                            transfer_name, mongo_task):
    """Creates a document in MongoDB, writes data"""

    mongo_doc_status = MongoDBTarget(document_id, 'status')
    mongo_transfer_name = MongoDBTarget(document_id,
                                        'transfer_name')
    mongo_mets_objid = MongoDBTarget(document_id,
                                     'mets_objid')
    mongo_timestamp = MongoDBTarget(document_id, 'timestamp')
    mongo_username = MongoDBTarget(document_id, 'username')
    mongo_wf_task = MongoDBTarget(document_id,
                                  'wf_tasks.move-transfer-to-workspace')

    mongo_transfer_name.write(transfer_name)
    mongo_mets_objid.write('')
    mongo_timestamp.write(datetime.datetime.utcnow().isoformat())
    mongo_username.write(username)
    mongo_doc_status.write(mongo_status)
    mongo_wf_task.write(mongo_task)

    return 0
