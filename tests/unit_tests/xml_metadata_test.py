"""Tests for :mod:`siptools_research.xml_metadata` module"""
from siptools_research.xml_metadata import XMLMetadataGenerator
try:
    import mock
except ImportError:
    from unittest import mock


def test_generate_xml_metadata_for_image_file():
    """Tests metadata XML generation for image file.
    :returns: ``None``
    """
    with mock.patch(
        'siptools.scripts.create_mix.create_mix_metadata'
    ) as mock_create_mix:
        file_path = '/foo/bar'
        file_metadata = {}
        file_metadata['file_characteristics'] = {'file_format': 'image/tiff'}
        generator = XMLMetadataGenerator(file_path,
                                         file_metadata)
        generator.create()
        mock_create_mix.assert_called_once_with(file_path)


def test_generate_xml_metadata_for_csv_file():
    """Tests metadata XML generation for CSV file.
    :returns: ``None``
    """
    with mock.patch(
        'siptools.scripts.create_addml.create_addml_metadata'
    ) as mock_create_addml:
        file_path = '/foo/bar'
        file_md = {}
        file_md['file_characteristics'] = {'file_format': 'text/csv',
                                           'csv_delimiter': ';',
                                           'csv_has_header': False,
                                           'encoding': 'UTF-8',
                                           'csv_record_separator': 'CR+LF',
                                           'csv_quoting_char': '\"'}
        file_md['file_path'] = '/foobar'
        generator = XMLMetadataGenerator(file_path,
                                         file_md)
        generator.create()
        mock_create_addml.assert_called_once_with(
            csv_file=file_path, delimiter=';', isheader=False,
            charset='UTF-8', record_separator='CR+LF', quoting_char='"',
            flatfile_name='/foobar'
        )


def test_generate_xml_metadata_for_audio_file():
    """Tests metadata XML generation for audio file.
    :returns: ``None``
    """
    with mock.patch(
        'siptools.scripts.create_audiomd.create_audiomd_metadata'
    ) as mock_create_audiomd:
        file_path = '/foo/bar'
        file_metadata = {}
        file_metadata['file_characteristics'] = {'file_format': 'audio/x-wav'}
        generator = XMLMetadataGenerator(file_path,
                                         file_metadata)
        generator.create()
        mock_create_audiomd.assert_called_once_with(file_path)


def test_generate_xml_metadata_for_video_file():
    """Tests metadata XML generation for audio file.
    """
    with mock.patch(
        'siptools.scripts.create_videomd.create_videomd_metadata'
    ) as mock_create_videomd:
        file_path = '/foo/bar'
        file_metadata = {
            'file_characteristics': {'file_format': 'video/ogg'}
        }
        generator = XMLMetadataGenerator(file_path, file_metadata)
        generator.create()
        mock_create_videomd.assert_called_once_with(file_path)
