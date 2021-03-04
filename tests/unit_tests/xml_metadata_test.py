"""Tests for :mod:`siptools_research.xml_metadata` module"""
import pytest

from siptools_research.xml_metadata import XMLMetadataGenerator

try:
    import mock
except ImportError:
    from unittest import mock


class DictMagicMock(mock.MagicMock):
    """
    Magic mock that fakes a dictionary with a single value
    """
    keys = lambda _: [0]
    values = lambda _: [{}]


@pytest.fixture(autouse=True)
def mock_combine_metadata(monkeypatch):
    """
    Monkeypatch `_combine_metadata` to a no-op
    """
    monkeypatch.setattr(
        "siptools_research.xml_metadata._combine_metadata",
        lambda results: results
    )


def test_generate_xml_metadata_for_image_file():
    """Tests metadata XML generation for image file.
    :returns: ``None``
    """
    streams = {
        0: {
            'mimetype': 'image/tiff',
            'stream_type': 'image'
        }
    }

    with mock.patch(
        'siptools.scripts.create_mix.create_mix_metadata',
        new_callable=DictMagicMock
    ) as mock_create_mix:
        file_path = '/foo/bar'
        file_metadata = {}
        file_metadata['file_characteristics'] = {
            'file_format': 'image/tiff',
            'streams': streams
        }
        generator = XMLMetadataGenerator(file_path,
                                         file_metadata)
        generator.create()
        mock_create_mix.assert_called_once_with(file_path, streams=streams)


def test_generate_xml_metadata_for_csv_file():
    """Tests metadata XML generation for CSV file.
    :returns: ``None``
    """
    with mock.patch(
        'siptools.scripts.create_addml.create_addml_metadata',
        new_callable=DictMagicMock
    ) as mock_create_addml:
        file_path = '/foo/bar'
        file_md = {}
        file_md['file_characteristics'] = {
            'file_format': 'text/csv',
            'csv_delimiter': ';',
            'csv_has_header': False,
            'encoding': 'UTF-8',
            'csv_record_separator': 'CR+LF',
            'csv_quoting_char': '\"',
            'streams': {
                0: {
                    "mimetype": "text/csv",
                    "index": 0,
                    "charset": "UTF-8",
                    "stream_type": "text",
                    "delimiter": ",",
                    "version": "(:unap)",
                    "separator": "\r\n",
                    "first_line": [
                        "Year",
                        "Make",
                        "Model",
                        "Length"
                    ]
                }
            }
        }

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
    streams = {
        0: {
            'mimetype': 'audio-xwav',
            'stream_type': 'audio',
            'bits_per_sample': '16'
        }
    }

    with mock.patch(
        'siptools.scripts.create_audiomd.create_audiomd_metadata',
        new_callable=DictMagicMock
    ) as mock_create_audiomd:
        file_path = '/foo/bar'
        file_metadata = {
            'file_characteristics': {
                'file_format': 'audio/x-wav',
                'streams': streams
            }
        }
        generator = XMLMetadataGenerator(file_path,
                                         file_metadata)
        generator.create()
        mock_create_audiomd.assert_called_once_with(file_path, streams=streams)


def test_generate_xml_metadata_for_video_file():
    """Tests metadata XML generation for video file.
    """
    streams = {
        0: {
            'mimetype': 'video/x-matroska',
            'stream_type': 'videocontainer',
        },
        1: {
            'mimetype': 'video/x-ffv',
            'stream_type': 'video',
        },
        2: {
            'mimetype': 'audio/flac',
            'stream_type': 'audio'
        }
    }

    mock_create_videomd = mock.patch(
        'siptools.scripts.create_videomd.create_videomd_metadata',
        new_callable=DictMagicMock
    )
    mock_create_audiomd = mock.patch(
        'siptools.scripts.create_audiomd.create_audiomd_metadata',
        new_callable=DictMagicMock
    )

    with mock_create_videomd as mock_create_videomd, \
            mock_create_audiomd as mock_create_audiomd:
        file_path = '/foo/bar'
        file_metadata = {
            'file_characteristics': {
                'file_format': 'video/x-matroska',
                'streams': streams
            }
        }
        generator = XMLMetadataGenerator(file_path, file_metadata)
        generator.create()
        mock_create_videomd.assert_called_once_with(file_path, streams=streams)
        mock_create_audiomd.assert_called_once_with(file_path, streams=streams)
