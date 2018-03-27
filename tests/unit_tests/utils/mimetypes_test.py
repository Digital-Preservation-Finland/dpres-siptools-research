'''
Created on 22 Mar 2018

@author: vagrant
'''
from siptools_research.utils import mimetypes
import mock


@mock.patch.object(mimetypes, "_get_mimetypes_filepath")
def test_text_csv_is_supported(mock__get_mimetypes_filepath):
    """Test that file having mime type text/csv with
    empty version
    """
    mock__get_mimetypes_filepath.return_value = \
        "include/etc/dpres_mimetypes.json"
    assert mimetypes.is_supported("text/csv", "") is True


@mock.patch.object(mimetypes, "_get_mimetypes_filepath")
def test_image_tiff_6_0_is_supported(mock__get_mimetypes_filepath):
    """Test that file having mime type image/tiff version 6.0
    is supported
    """
    mock__get_mimetypes_filepath.return_value = \
        "include/etc/dpres_mimetypes.json"
    assert mimetypes.is_supported("image/tiff", "6.0") is True


@mock.patch.object(mimetypes, "_get_mimetypes_filepath")
def test_image_tiff_6_x_is_supported(mock__get_mimetypes_filepath):
    """Test that file having mime type image/tiff version 6.X
    is not supported
    """
    mock__get_mimetypes_filepath.return_value = \
        "include/etc/dpres_mimetypes.json"
    assert mimetypes.is_supported("image/tiff", "6.X") is False


@mock.patch.object(mimetypes, "_get_mimetypes_filepath")
def test_some_carbage_is_not_supported(mock__get_mimetypes_filepath):
    """Test that file having mime type text/garbage and
    is not supported
    """
    mock__get_mimetypes_filepath.return_value = \
        "include/etc/dpres_mimetypes.json"
    assert mimetypes.is_supported("text/garbage", "1.0") is False
