"""Base task classes for the workflow tasks"""

import os
import luigi


class WorkflowTask(luigi.Task):
    """Common base class for all workflow tasks.

    Usage::

        class Task(WorkflowTask):

            def requires(self):
                ...
            def output(self):
                ...
            def run(self):
                ...

        task = task(workspace=`<workspace-path>`)

    :self.workspace: Full path to unique self.workspace directory for the task.

    """

    workspace = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.Task's __init__ and sets the workspace we get as a
        parameter.

        """

        # luigi.Task does not allow adding attributes after __init__(). Add
        # attribute first, then set the attribute to value from self.workspace
        # parameter

        super(WorkflowTask, self).__init__(*args, **kwargs)
        self.document_id = os.path.basename(self.workspace)
        self.task_name = self.__class__.__name__


class WorkflowExternalTask(luigi.ExternalTask):
    """Common base class for all tasks that are executed externally from this
    process and task does not implement the run() method, only output() and
    requires() methods.

    Usage::

        class Task(WorkflowExternalTask):

            def requires(self):
                ...
            def output(self):
                ...
            def run(self):
                ...

        task = task(workspace=`<workspace-path>`)

    :self.workspace: Full path to unique self.workspace directory for the task.

    """

    workspace = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.ExternalTask's __init__ and sets the workspace we get as
        a parameter.

        """

        # luigi.Task does not allow adding attributes after __init__(). Add
        # attribute first, then set the attribute to value from self.workspace
        # parameter

        super(WorkflowExternalTask, self).__init__(*args, **kwargs)


class WorkflowWrapperTask(luigi.WrapperTask):
    """Common base class for all workflow tasks which execute other tasks, but
    does not have implementation itself.

    Usage::

        class Task(WorkflowWrapperTask):

            def requires(self):
                ...

        task = task(workspace=`<workspace-path>`)

    :self.workspace: Full path to unique self.workspace directory for the task.

    """

    workspace = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """Calls luigi.Task's __init__ and sets the workspace we get as a
        parameter.

        """

        # luigi.Task does not allow adding attributes after __init__(). Add
        # attribute first, then set the attribute to value from self.workspace
        # parameter

        super(WorkflowWrapperTask, self).__init__(*args, **kwargs)
