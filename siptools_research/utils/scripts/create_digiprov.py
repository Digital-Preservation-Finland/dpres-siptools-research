# coding=utf-8
"""Creates provenance information as PREMIS event and PREMIS agent
files from Metax metadata using siptools premis_event script.
"""

import sys
import argparse
from siptools.scripts import premis_event
from siptools_research.utils.metax import Metax


def parse_arguments(arguments):
    """ Create arguments parser and return parsed
    command line arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_id", type=str, help="Metax id of dataset")
    parser.add_argument("--workspace", dest="workspace", type=str,
                        default="./workspace", help="Workspace directory")

    return parser.parse_args(arguments)


def create_premis_event(dataset_id, workspace):
    """Gets metada from Metax and calls siptools premis_event script."""

    metadata = Metax().get_data('datasets', dataset_id)
    event_type = metadata["research_dataset"]["provenance"][0]["type"]\
        ["pref_label"][0]["en"]
    event_datetime = metadata["research_dataset"]["provenance"][0]\
        ["temporal"][0]["start_date"]
    event_detail = metadata["research_dataset"]["provenance"][0]\
        ["description"]["en"]

    premis_event.main([
        event_type, event_datetime,
        "--event_detail", event_detail,
        "--workspace", workspace
    ])


def main(arguments=None):
    """The main method for argparser"""
    args = parse_arguments(arguments)

    create_premis_event(args.dataset_id, args.workspace)

    return 0


if __name__ == "__main__":
    RETVAL = main()
    sys.exit(RETVAL)
