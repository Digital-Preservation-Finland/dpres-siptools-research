# coding=utf-8
"""Creates provenance information as PREMIS event and PREMIS agent
files about the creation of EAD3 finding aid file from SÄHKE2
metadata using siptools premis_event script.
"""

import sys
import argparse
import datetime

import lxml.etree as ET

import ipt.version

from siptools.scripts import premis_event
from siptools.xml.namespaces import NAMESPACES


def parse_arguments(arguments):
    """ Create arguments parser and return parsed
    command line arguments.
    """
    parser = argparse.ArgumentParser(description='Tool for '
                                     'creating premis events')
    parser.add_argument('digital_object', type=str, help='Target for create '
                        'event')
    parser.add_argument('--workspace', dest='workspace', type=str,
                        default='./workspace', help="Workspace directory")

    return parser.parse_args(arguments)


def create_premis_event(digital_object, workspace):
    """Calls siptools premis_event script."""
    event_type = 'migration'
    event_datetime = datetime.datetime.utcnow().isoformat()
    event_detail = ('Creation of EAD3 finding aid file by automatic '
                    'migration from SAHKE2 transfer metadata')
    event_outcome = 'success'
    event_outcomedetail = ('EAD3 finding aid file created. Recordid: %s' %
                           get_ead3_id(digital_object))
    agent_name = 'import_sahke2.py-%s' % (ipt.version.__version__)
    agent_type = 'software'

    premis_event.main([event_type, event_datetime,
                       '--event_detail', event_detail,
                       '--event_outcome', event_outcome,
                       '--event_outcome_detail', event_outcomedetail,
                       '--workspace', workspace,
                       '--agent_name', agent_name,
                       '--agent_type', agent_type])


def get_ead3_id(digital_object):
    """Gets the identifier for the EAD3 finding aid file."""
    tree = ET.parse(digital_object)
    root = tree.getroot()
    identifier = root.xpath(
        "//ead3:recordid[1]", namespaces=NAMESPACES)[0].text

    return identifier


def main(arguments=None):
    """The main method for argparser"""
    args = parse_arguments(arguments)

    create_premis_event(args.digital_object, args.workspace)

    return 0


if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)
