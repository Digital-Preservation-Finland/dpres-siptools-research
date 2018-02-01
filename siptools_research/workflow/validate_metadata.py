"""Luigi task that validates metadata provided by Metax."""

import os
from luigi import LocalTarget
import jsonschema
import siptools_research.utils.metax_schemas as metax_schemas
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.utils.metax import Metax
from siptools_research.workflow.create_workspace import CreateWorkspace
from siptools_research.luigi.task import WorkflowTask
from siptools_research.luigi.task import InvalidMetadataError
import lxml
from subprocess import Popen, PIPE
from siptools.xml.mets import NAMESPACES
import tempfile

SCHEMATRONS = {
    'image': {'ns': NAMESPACES['mix'], 'schematron':
              '/usr/share/dpres-xml-schemas/schematron/mets_mix.sch'}
    }

SHEM_ERR = "Schematron metadata validation failed: %s. File: %s"
MISS_XML_ERR = "Missing XML metadata for file: %s"
INV_NS_ERR = "Invalid XML namespace: %s"


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
        return LocalTarget(os.path.join(self.logs_path,
                                        'validate-metadata.log'))

    def run(self):
        with self.output().open('w') as log:
            with redirect_stdout(log):

                # Get dataset metadata from Metax
                self.metax_client = Metax(self.config)
                dataset_metadata = self.metax_client.get_data('datasets',
                                                              self.dataset_id)

                # Validate dataset metadata
                try:
                    jsonschema.validate(dataset_metadata,
                                        metax_schemas.DATASET_METADATA_SCHEMA)
                except jsonschema.ValidationError as exc:
                    raise InvalidMetadataError(exc)

                # Get dataset metadata for each listed file, and validate file
                # metadata
                self.__validate_dataset_metadata_files(dataset_metadata)
                # Validate file metadata for each file in dataset files
                self.__validate_xml_file_metadata()

    def __validate_dataset_metadata_files(self, dataset_metadata):
                for dataset_file in \
                        dataset_metadata['research_dataset']['files']:
                    file_id = dataset_file['identifier']
                    file_metadata = self.metax_client.get_data('files',
                                                               file_id)
                    # Validate dataset metadata
                    try:
                        jsonschema.validate(file_metadata,
                                            metax_schemas.FILE_METADATA_SCHEMA)
                    except jsonschema.ValidationError as exc:
                        raise InvalidMetadataError(exc)

    def __validate_xml_file_metadata(self):
        for file_metadata in \
                self.metax_client.get_dataset_files(self.dataset_id):
            try:
                jsonschema.validate(file_metadata,
                                    metax_schemas.FILE_METADATA_SCHEMA)
            except jsonschema.ValidationError as exc:
                raise InvalidMetadataError(exc)
            file_format_prefix = file_metadata['file_format'].split('/')[0]
            if file_format_prefix in SCHEMATRONS:
                file_id = file_metadata['identifier']
                xmls = self.metax_client.get_xml('files',
                                                 file_id)
                for ns_url in xmls:
                    if ns_url not in NAMESPACES.values():
                        raise TypeError(INV_NS_ERR % ns_url)
                if SCHEMATRONS[file_format_prefix]['ns'] not in xmls:
                    raise InvalidMetadataError(MISS_XML_ERR %
                                               file_id)
                self.__validate_with_schematron(file_format_prefix,
                                                file_id,
                                                xmls)

    def __validate_with_schematron(self, file_format_prefix, file_id, xmls):
        with tempfile.NamedTemporaryFile() as temp:
            ns = xmls[SCHEMATRONS[file_format_prefix]['ns']]
            temp.write(lxml.etree.tostring(ns).strip())
            temp.seek(0)
            schem = SCHEMATRONS[file_format_prefix]['schematron']
            proc = Popen(['check-xml-schematron-features', '-s',
                          schem, temp.name],
                         stdout=PIPE,
                         stderr=PIPE,
                         shell=False,
                         cwd=None,
                         env=None)
            proc.communicate()
            if proc.returncode != 0:
                raise InvalidMetadataError(
                    SHEM_ERR % (proc.returncode, file_id))
