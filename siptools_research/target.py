"""Luigi targets"""

import os

from luigi import LocalTarget
from luigi.contrib.mongodb import MongoCellTarget
from pymongo import MongoClient


class TaskReportTarget(LocalTarget):

    """Luigi target for PREMIS task reports"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget with custom report path"""

        filename = 'task-report-%s.xml' % event_name
        path = os.path.join(workspace, 'reports', filename)
        print "TaskReportTarget:%s" % path
        LocalTarget.__init__(self, path)


class TaskFileTarget(LocalTarget):

    """Luigi target for task files"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget in workspace"""

        path = os.path.join(workspace, 'task-output-files', event_name)
        print "TaskFileTarget:%s" % path
        LocalTarget.__init__(self, path)


def mongo_settings():
    """Variables for mongo_db"""
    host = 'localhost'
    mongo_db = 'siptools-research'
    mongo_col = 'workflow'
    mongo_client = MongoClient(host)

    return mongo_client, mongo_db, mongo_col


class MongoDBTarget(MongoCellTarget):
    """MongoCellTarget
    """

    def __init__(self, document_id, mongo_field):
        (mongo_client, mongo_db, mongo_col) = mongo_settings()
        MongoCellTarget.__init__(self, mongo_client, mongo_db, mongo_col,
                                 document_id, mongo_field)


class TaskLogTarget(LocalTarget):

    """Luigi target for generic log files"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget with custom log path"""

        filename = 'task-%s.log' % event_name
        path = os.path.join(workspace, 'logs', filename)
        print "*** TaskLogTarget:%s" % path
        LocalTarget.__init__(self, path)


class IngestReportTarget(LocalTarget):

    """Luigi target for ingest XML reports"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget with custom report path"""

        filename = 'ingest-report-%s.xml' % event_name
        path = os.path.join(workspace, 'reports', filename)
        LocalTarget.__init__(self, path)


class IngestReportTargetHtml(LocalTarget):

    """Luigi target for ingest HTML reports"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget with custom report path"""

        filename = 'ingest-report-%s.html' % event_name
        path = os.path.join(workspace, 'reports-html', filename)
        LocalTarget.__init__(self, path)
