"""Luigi targets"""

import os

import luigi.contrib.ssh
from luigi import LocalTarget
from luigi.contrib.mongodb import MongoCellTarget
import pymongo


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
    mongo_client = pymongo.MongoClient(host)

    return mongo_client, mongo_db, mongo_col


class MongoTaskResultTarget(MongoCellTarget):
    """Target that exists when value of mongodb field:

    db.``document_id``.wf_tasks.``mongo_field``.result

    has value "success"
    """

    # TODO: This is ugly but it works. Should be refactored at some point...
    def __init__(self, document_id, taskname):
        (mongo_client, mongo_db, mongo_col) = mongo_settings()
        MongoCellTarget.__init__(self,
                                 mongo_client,
                                 mongo_db,
                                 mongo_col,
                                 document_id,
                                 "workflow_tasks.%s.result" % taskname)

    def exists(self):
        return self.read() == "success"


class TaskLogTarget(LocalTarget):

    """Luigi target for generic log files"""

    def __init__(self, workspace, event_name):
        """Setup LocalTarget with custom log path"""

        filename = 'task-%s.log' % event_name
        path = os.path.join(workspace, 'logs', filename)
        print "*** TaskLogTarget:%s" % path
        LocalTarget.__init__(self, path)


class RemoteAnyTarget(luigi.contrib.ssh.RemoteTarget):
    """Modified version of luigi.contrib.ssh.RemoteTarget. A list of possible
    file paths is given instead of one path. The target exists if any of those
    paths exist at remote server."""
    def exists(self):
        """Returns ``True`` if any of paths exist."""
        return any([self.fs.exists(p) for p in self.path])

    def existing_paths(self):
        """Returns the paths that exists."""
        existing_paths = []
        for path in self.path:
            if self.fs.exists(path):
                existing_paths.append(path)
        return existing_paths
