"""Utility functions for the Luigi tasks"""

import traceback

from siptools_research.move_sip import FailureLog
from siptools_research.mongo_database import write_document_status, \
        write_mongodb_task


def task_failed(document_id, task_name, workspace, message=None):
    """Writes failed task info to Mongo DB, sets status as rejected,
    move rejected transfer to users home/rejected folder and marks
    transfer to be ready for cleanup.
    """
    if message is None:
        message = traceback.format_exc()

    task_result = 'failure'
    write_document_status(document_id, 'rejected')

    write_mongodb_task(document_id, task_name,
                       task_result, message)

    failed_log = FailureLog(workspace).output()
    with failed_log.open('w') as outfile:
        outfile.write('Task %s failed.' % task_name)

    return 0
