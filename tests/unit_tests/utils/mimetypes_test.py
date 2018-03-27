'''
Created on 22 Mar 2018

@author: vagrant
'''
from siptools_research.utils import mimetypes


def test_text_csv_is_supported():
    """Test that file having mime type text/plain and
    encoding ISO-8859-15 is supported
    """
    assert mimetypes.is_supported("text/csv", "") is True


def test_some_carbage_is_not_supported():
    """Test that file having mime type text/garbage and
    is not supported
    """
    assert mimetypes.is_supported("text/garbage", "") is False
