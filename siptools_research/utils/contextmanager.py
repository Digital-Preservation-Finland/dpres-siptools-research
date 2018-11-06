"""Implement functionality of contextlib.redirect_stdout() function that is
availabale for python 3 but not python 2.7"""
import sys
import contextlib


@contextlib.contextmanager
def redirect_stdout(target):
    """Context manager for temporarily redirecting sys.stdout to another file
    or file-like object.

    :param target: File object
    :returns: ``None``
    """
    original = sys.stdout
    sys.stdout = target
    yield
    sys.stdout = original
