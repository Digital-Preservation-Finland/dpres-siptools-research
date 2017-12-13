# encoding=utf8
"""Luigi task that creates structure map."""

import os
import json
import lxml.etree as ET
import mets
import luigi
from siptools.scripts import compile_structmap
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask
from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import \
    CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata


class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.

    :returns: dict
    """
    success_message = "Structure map created"
    failure_message = "Structure map could not be created"

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                CreateDescriptiveMetadata(workspace=self.workspace,
                                          dataset_id=self.dataset_id,
                                          config=self.config),
                "Create provenance information completed":
                CreateProvenanceInformation(workspace=self.workspace,
                                            dataset_id=self.dataset_id,
                                            config=self.config),
                "Create technical metadata completed":
                CreateTechnicalMetadata(workspace=self.workspace,
                                        dataset_id=self.dataset_id,
                                        config=self.config)}

    def output(self):
        """Writes log to ``logs/create-struct-map`` file

        :returns: LocalTarget
        """
        return luigi.LocalTarget(os.path.join(self.logs_path,
                                              'create-struct-map'))

    def run(self):
        """Creates a METS structural map file based on a folder structure. Top
        folder is given as a parameter.

        :returns: None

        """
        # Redirect stdout to logfile
        with self.output().open('w') as log:
            with redirect_stdout(log):
                #create structmap
                compile_structmap.main(['--workspace',
                                        self.sip_creation_path])
                # pylint: disable=no-member
                filesec_xml = ET.parse(os.path.join(self.sip_creation_path,
                                                    'filesec.xml'))

                # in getfiles task the files are mapped with categories and
                # wrote in 'logical_struct' file 'logical_struct' file is read
                # and the logical struct map is generated based on it

                cat_file = open(os.path.join(self.sip_creation_path,
                                             'logical_struct'))
                categories = json.loads(cat_file.read())
                # create logical structmap
                structmap = mets.structmap(type_attr='Fairdata-logical')

                # Read the previously (in compile_structmap) generated
                # physical structmap from file
                struct = ET.parse(os.path.join(self.sip_creation_path,
                                               'structmap.xml'))
                # get dmd id
                root = struct.getroot()
                old_struct = root[0][0]
                dmdsec_id = [old_struct.attrib['DMDID']]

                wrapper_div = mets.div(type_attr='logical', dmdid=dmdsec_id)
                print "wrap %s " % ET.tostring(wrapper_div)
                for category in categories:
                    div_cat = mets.div(type_attr=category, dmdid=dmdsec_id)
                    for file_ in categories.get(category):
                        filename = file_
                        fileid = get_fileid(filename, filesec_xml)
                        div_cat.append(mets.fptr(fileid))
                    wrapper_div.append(div_cat)
                structmap.append(wrapper_div)

                # Add the logical structmap into the same file where physical
                # struct map already is
                start = mets.structmap(type_attr='Fairdata-physical')
                start.append(old_struct)
                new_struct = ET.tostring(start)
                new_struct += ET.tostring(structmap, pretty_print=True,
                                          xml_declaration=False,
                                          encoding='UTF-8')
                print "new struct %s" %new_struct

                with open(os.path.join(self.sip_creation_path,
                                       'structmap.xml'), 'r+') as struct_file:
                    struct_file.write(new_struct)
                    struct_file.close()


def get_fileid(filename, filesec_xml):
    """get file id from filesec by filename"""
    fileid = None
    root = filesec_xml.getroot()

    files = root[0][0]
    for file_ in files:
        for file__ in file_:
            if str(file__.get('{http://www.w3.org/1999/xlink}href'))[7:] \
                    == filename:
                print "file found %s " % file_.get('ID')
                fileid = file_.get('ID')
                break

    return fileid
