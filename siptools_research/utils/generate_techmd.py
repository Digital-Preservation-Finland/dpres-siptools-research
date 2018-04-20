# coding=utf-8
"""Utils for 
"""

import os
import sys
from siptools.scripts import import_object, create_mix

def generate_techmd(filename):
    """Generates technical md
    """
    metadata = siptools.scripts.import_object.metadata_info(filename)
    mimetype = metadata["format"]["mimetype"]
    mimemaintype = mimemaintype = mimetype.rsplit('/', 1)[0]
    if mimemaintype = "image":
        mix_techmd = siptools.scripts.create_techmd(filename)


    return metadata



def main(arguments=None):



if __name__ == '__main__':
    RETVAL = main()
    sys.exit(RETVAL)
