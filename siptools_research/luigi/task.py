"""Base task classes for the workflow tasks"""

import os
import luigi


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks. In addition to functionality
    of normal luigi Task, every workflow task has two parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.

    WorkflowTask also has two extra instance variables that can be used to
    identify the task and current workflow, forexample when storing workflow
    status information to database:

    :task_name: Automatically generated name for the task.
    :document_id: Identifier of the workflow. Generated from the name of
    workspace, which should be unique
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.Task's __init__ and sets additional instance variables.
        """
        super(WorkflowTask, self).__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__
        self.sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')

class WorkflowExternalTask(luigi.ExternalTask):
    """Common base class for all tasks that are executed externally from this
    process and task does not implement the run() method, only output() and
    requires() methods. In addition to functionality of normal luigi
    ExternalTask, every task has two parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()


class WorkflowWrapperTask(luigi.WrapperTask):
    """Common base class for all workflow tasks which execute other tasks, but
    does not have implementation itself.  In addition to functionality of
    normal luigi WrapperTask, every task has two parameters:

    :workspace: Full path to unique self.workspace directory for the task.
    :dataset_id: Metax dataset id.
    """

    workspace = luigi.Parameter()
    dataset_id = luigi.Parameter()
