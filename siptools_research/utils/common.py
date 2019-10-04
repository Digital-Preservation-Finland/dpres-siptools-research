"""Common utility funcions"""
import os
import sys
from contextlib import contextmanager


@contextmanager
def silence_stdout():
    """Forward stdout to /dev/null"""
    new_target = open(os.devnull, "w")
    old_target = sys.stdout
    sys.stdout = new_target
    try:
        yield new_target
    finally:
        sys.stdout = old_target
