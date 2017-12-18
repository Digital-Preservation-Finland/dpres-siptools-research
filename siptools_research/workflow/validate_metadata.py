"""Luigi task that validates metadata provided by Metax."""

import os
from luigi import LocalTarget
from jsonschema import ValidationError
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.validate_metadata import validate_dataset_metadata
from siptools_research.utils.metax import Metax
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.task import InvalidMetadataError

class ValidateMetadata(WorkflowTask):
    """Gets metadata from Metax and validates it. Requires workspace directory
    to be created. Writes log to ``logs/validate-metadata.log``
    """
    success_message = "Metax metadata is valid"
    failure_message = "Metax metadata could not be validated"

    def requires(self):
        return CreateWorkspace(workspace=self.workspace,
                               dataset_id=self.dataset_id,
                               config=self.config)

    def output(self):
        return  LocalTarget(os.path.join(self.logs_path,
                                         'validate-metadata.log'))

    def run(self):
        with self.output().open('w') as log:
            with redirect_stdout(log):

                metax_client = Metax(self.config)
                dataset_metadata = metax_client.get_data('datasets',
                                                         self.dataset_id)
                try:
                    validate_dataset_metadata(dataset_metadata)
                except ValidationError as exc:
                    raise InvalidMetadataError(exc)
