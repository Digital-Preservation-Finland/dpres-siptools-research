# encoding=utf8
"""required tasks to create SIPs from transfer"""

import os
import sys
import traceback
import lxml.etree as ET
import mets
import json
import xml_helpers.utils as h
from xml_helpers.utils import decode_utf8
from datetime import datetime
from luigi import Parameter


from siptools.scripts import compile_structmap

from siptools.xml.mets import NAMESPACES

from siptools_research.luigi.target import MongoTaskResultTarget
from siptools_research.utils import database
from siptools_research.utils.contextmanager import redirect_stdout
from siptools_research.luigi.task import WorkflowTask

from siptools_research.workflow.create_dmdsec import CreateDescriptiveMetadata
from siptools_research.workflow.create_digiprov import CreateProvenanceInformation
from siptools_research.workflow.create_techmd import CreateTechnicalMetadata



class CreateStructMap(WorkflowTask):
    """Create METS fileSec and structMap files.
    """
    workspace = Parameter()

    def requires(self):
        """Requires dmdSec file, PREMIS object files, PREMIS
        event files and PREMIS agent files
        """
        return {"Create descriptive metadata completed":
                CreateDescriptiveMetadata(workspace=self.workspace,
                                          dataset_id=self.dataset_id),
                "Create provenance information completed":
                CreateProvenanceInformation(workspace=self.workspace,
                                            dataset_id=self.dataset_id),
                "Create technical metadata completed":
                CreateTechnicalMetadata(workspace=self.workspace,
                                        dataset_id=self.dataset_id)}

    def output(self):
        """Outputs a task file"""
        return MongoTaskResultTarget(document_id=self.document_id,
                                     taskname=self.task_name)

    def run(self):

        """Creates a METS structural map file based on a folder structure. Top folder is given as a parameter.
        If unsuccessful writes an error message into mongoDB, updates
        the status of the document and rejects the package. The rejected
        package is moved to the users home/rejected directory.

        :returns: None

        """
        sip_creation_path = os.path.join(self.workspace, 'sip-in-progress')
        document_id = os.path.basename(self.workspace)
        task_result = None

        structmap_log = os.path.join(self.workspace,
                                  "logs",
                                  'create-struct-map')
        # Redirect stdout to logfile
        #with open(structmap_log, 'w') as log:
           # with redirect_stdout(log):

        try:

                    workspace = self.workspace
                    #create structmap
                    compile_structmap.main([
                            '--workspace', sip_creation_path])
                    task_result = 'success'
                    task_messages = "Structrural map and filesec created."
                    filesec_xml = ET.parse(os.path.join(self.workspace, 'sip-in-progress', 'filesec.xml'))
                      
                    #in getfiles task the files are mapped with categories and wrote in 'logical_struct' file
		    #'logical_struct' file is read and the logical struct map is generated based on it  

                    cat_file = open(os.path.join(self.workspace,
                                                 'sip-in-progress','logical_struct'))
                    categories = json.loads(cat_file.read())
                    #create logical structmap
                    structmap = mets.structmap(type_attr='Fairdata-logical')

                    #Read the previously (in compile_structmap) generated  physical structmap from file
                    struct = ET.parse(os.path.join(workspace,
                                                   'sip-in-progress','structmap.xml'))
                    # get dmd id 
                    root = struct.getroot()       
                    old_struct = root[0][0]
                    dmdsec_id = [old_struct.attrib['DMDID']]
                     
                    wrapper_div = mets.div(type_attr='logical', dmdid=dmdsec_id)
                    print "wrap %s "%ET.tostring(wrapper_div)
                    for category in categories:
                        div_cat = mets.div(type_attr=category, dmdid=dmdsec_id)
                        for file in categories.get(category):
                             filename = file 
                             fileid = get_fileid(filename, filesec_xml)                   
                             div_cat.append(mets.fptr(fileid))
                        wrapper_div.append(div_cat)
                    structmap.append(wrapper_div)                     
 
                    #Add the logical structmap into the same file where physical struct map already is
                    start = mets.structmap(type_attr='Fairdata-physical')
		    start.append(old_struct)
                    new_struct = ET.tostring(start) 
	            new_struct += ET.tostring(structmap, pretty_print=True, xml_declaration=False, encoding='UTF-8')
                    print "new struct %s" %new_struct

                    with open(os.path.join(workspace,
                                 'sip-in-progress','structmap.xml'), 'r+') as struct_file:                    
                        struct_file.write(new_struct)
                        struct_file.close()   
                    


        except KeyError as ex:
                   task_result = 'failure'
                   task_messages = 'Could not create structrural map, '\
                                    'element "%s" not found from metadata.'\
                                    % exc.message


        finally:
                    if not task_result:
                        task_result = 'failure'
                        task_messages = "Creation of structmap and filesec "\
                                        "failed due to unknown error."
                    database.add_event(self.document_id,
                           self.task_name,
                           task_result,
                           task_messages)


#get file id from filesec by filename
def get_fileid(filename, filesec_xml):
    fileid = None 
    root = filesec_xml.getroot()
     
    #for child in root:
    #    for file in root.iter('file'):
                             
    files = root [0][0]
    for file in files:
        for f in file:
            if str(f.get('{http://www.w3.org/1999/xlink}href'))[7:] == filename :
                print "file found %s " % file.get('ID')
                fileid = file.get('ID')
                break

    return fileid

