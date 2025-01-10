"""Tests for :mod:`siptools_research.metadata_generator` module."""
import copy
import shutil
from collections import defaultdict
from pathlib import Path

import pytest
from metax_access import DatasetNotAvailableError
from metax_access.metax import Metax
from requests.exceptions import HTTPError

import tests.metax_data.datasets
import tests.metax_data.files
import tests.utils
from siptools_research.exceptions import (
    InvalidFileError,
    InvalidFileMetadataError,
)
from siptools_research.metadata_generator import generate_metadata


@pytest.fixture(scope="function")
def file_v3_mock(mocker):
    """
    Mock `Metax.patch_file` in metax-access in order to inspect
    V3 file metadata submitted to Metax
    """
    yield mocker.spy(Metax, "patch_file_characteristics")


def _create_file_v2_http_mock(requests_mock):
    # This would preferably be a fixture instead of a method.
    # However, 'add_metax_v2_dataset' also adds its own PATCH file endpoint
    # mock and we need to run this afterwards to override it.
    return requests_mock.patch(
        "/rest/v2/files/pid:urn:identifier",
        json={}
    )


def test_generate_metadata(config, file_v3_mock, requests_mock, tmp_path):
    """Test metadata generation.

    Generates metadata for text file. Checks that expected request is
    sent to Metax.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param tmp_path: Temporary path
    """
    # create mocked dataset in Metax
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_metadata["file_path"] = "textfile"
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file_metadata])
    file_v2_http_mock = _create_file_v2_http_mock(requests_mock)

    # Create a text file in temporary directory
    tmp_file_path = tmp_path / "textfile"
    tmp_file_path.write_text("foo")

    # Generate metadata
    generate_metadata("dataset_identifier",
                      root_directory=tmp_path,
                      config=config)

    # V3: verify the file characteristics that were provided to metax-access
    assert len(file_v3_mock.mock_calls) == 1
    call = file_v3_mock.mock_calls[0]
    file_characteristics = call.args[2]["characteristics"]
    file_char_ext = call.args[2]["characteristics_extension"]

    assert file_characteristics == {
        "file_format_version": {
            "file_format": "text/plain"
        },
        "encoding": "UTF-8"
    }
    assert file_char_ext["streams"] == {
        0: {
            "charset": "UTF-8",
            "index": 0,
            "mimetype": "text/plain",
            "stream_type": "text",
            "version": "(:unap)"
        }
    }
    # It does not make sense to validate "info", but at least it should
    # not be empty
    assert file_char_ext["info"]
    assert file_char_ext["mimetype"] == "text/plain"
    assert file_char_ext["version"] ==  "(:unap)"
    assert file_char_ext["grade"] == "fi-dpres-recommended-file-format"

    # V2: ditto
    json = file_v2_http_mock.last_request.json()
    assert json["file_characteristics"] == {
        "file_format": "text/plain",
        "encoding": "UTF-8",
    }
    assert json["file_characteristics_extension"]["streams"] == {
        "0": {
            "charset": "UTF-8",
            "index": 0,
            "mimetype": "text/plain",
            "stream_type": "text",
            "version": "(:unap)"
        }
    }
    assert json["file_characteristics_extension"]["info"]
    assert json["file_characteristics_extension"]["mimetype"] == "text/plain"
    assert json["file_characteristics_extension"]["version"] ==  "(:unap)"
    assert json["file_characteristics_extension"]["grade"] \
        == "fi-dpres-recommended-file-format"


@pytest.mark.parametrize(
    (
        'path',
        'expected_v3_file_characteristics',
        'expected_v2_file_characteristics',
        'expected_stream_type'
    ),
    [
        # Text file should have encoding, but not format_version
        (
            'tests/data/sample_files/text_plain_UTF-8',
            {
                'file_format_version': {'file_format': 'text/plain'},
                'encoding': 'UTF-8',
            },
            {
                'file_format': 'text/plain',
                'encoding': 'UTF-8',
            },
            'text'
        ),
        (
            'tests/data/sample_files/image_png.png',
            {
                'file_format_version': {
                    'file_format': 'image/png',
                    'format_version': '1.2'
                }
            },
            {
                'file_format': 'image/png',
                'format_version': '1.2',
            },
            'image'
        ),
        (
            'tests/data/sample_files/image_tiff_large.tif',
            {
                'file_format_version': {
                    'file_format': 'image/tiff',
                    'format_version': '6.0'
                }
            },
            {
                'file_format': 'image/tiff',
                'format_version': '6.0',
            },
            'image'
        ),
        # WAV file should not have container stream according to DPS
        # specs
        (
            'tests/data/sample_files/audio_x-wav.wav',
            {
                'file_format_version': {'file_format': 'audio/x-wav'}
            },
            {
                'file_format': 'audio/x-wav',
            },
            'audio'
        ),
        # The first stream of matroska file should be container
        (
            'tests/data/sample_files/video_ffv1.mkv',
            {
                'file_format_version': {
                    'file_format': 'video/x-matroska',
                    'format_version': '4'
                }
            },
            {
                'file_format': 'video/x-matroska',
                'format_version': '4',
            },
            'videocontainer'
        ),
        # Ensure that problematic ODF formats (especially ODF Formula)
        # are detected correctly
        (
            'tests/data/sample_files/opendocument_text.odt',
            {
                'file_format_version': {
                    'file_format': 'application/vnd.oasis.opendocument.text',
                    'format_version': '1.2'
                }
            },
            {
                'file_format': 'application/vnd.oasis.opendocument.text',
                'format_version': '1.2',
            },
            'binary'
        ),
        (
            'tests/data/sample_files/opendocument_formula.odf',
            {
                'file_format_version': {
                    'file_format': 'application/vnd.oasis.opendocument.formula',
                    'format_version': '1.2'
                }
            },
            {
                'file_format': 'application/vnd.oasis.opendocument.formula',
                'format_version': '1.2',
            },
            'binary'
        )
    ]
)
def test_file_format_detection(
        config, file_v3_mock, requests_mock, path,
        expected_v2_file_characteristics,
        expected_v3_file_characteristics,
        expected_stream_type, tmp_path):
    """Test file format detection.

    Generates metadata for a dataset that contains one file, and checks
    that file format is detected correctly.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param path: path to the file for which the metadata is created
    :param expected_file_characteristics: expected file_characteristics
    :param expected_stream_type: expected type of first stream in
                                 file_characteristics_extension
    :param tmp_path: Temporary directory
    """
    # create mocked dataset in Metax
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_path = Path('/path/to') / Path(path).name
    file_metadata['file_path'] = str(file_path)
    tests.utils.add_metax_v2_dataset(
        requests_mock, files=[file_metadata]
    )
    file_v2_http_mock = _create_file_v2_http_mock(requests_mock)

    # Copy the file to temporary directory
    tmp_file_path = tmp_path / file_path.relative_to('/')
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy(path, tmp_file_path)

    # Generate metadata
    generate_metadata('dataset_identifier',
                      root_directory=tmp_path,
                      config=config)

    # V3: test the PATCH request was sent
    assert len(file_v3_mock.mock_calls) == 1
    call = file_v3_mock.mock_calls[0]

    # V3: verify the file characteristics that were sent to Metax
    file_characteristics \
        = call.args[2]['characteristics']
    assert file_characteristics == expected_v3_file_characteristics

    # Check that at least the mimetype and stream type of the are
    # correct in the first stream of file_characteristics_extension
    file_char_ext = call.args[2]['characteristics_extension']
    assert file_char_ext['streams'][0]['mimetype'] \
        == expected_v3_file_characteristics['file_format_version']['file_format']
    assert file_char_ext['streams'][0]['stream_type'] == expected_stream_type

    # V2: ditto
    file_characteristics \
        = file_v2_http_mock.last_request.json()['file_characteristics']
    assert file_characteristics == expected_v2_file_characteristics

    file_char_ext = file_v2_http_mock.last_request.json()[
        'file_characteristics_extension'
    ]
    assert file_char_ext['streams']['0']['mimetype'] \
        == expected_v2_file_characteristics['file_format']
    assert file_char_ext['streams']['0']['stream_type'] == expected_stream_type


def test_generate_metadata_video_streams(config, file_v3_mock,
                                         requests_mock, tmp_path):
    """Test metadata generation for a video file.

    Generates file characteristics for a video file with multiple
    streams.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param tmp_path: Temporary directory
    """
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        files=[tests.metax_data.files.BASE_FILE])

    tmp_file_path = tmp_path / "path/to/file"
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/video_ffv1.mkv', tmp_file_path)
    file_v2_http_mock = _create_file_v2_http_mock(requests_mock)

    generate_metadata('dataset_identifier', tmp_path, config)

    # V3: test that correct streams were found
    assert len(file_v3_mock.mock_calls) == 1
    call = file_v3_mock.mock_calls[0]

    file_char_ext = call.args[2]['characteristics_extension']

    # Four different streams found
    assert {0, 1, 2, 3} == set(file_char_ext['streams'].keys())

    streams_by_type = defaultdict(list)
    for stream in file_char_ext['streams'].values():
        streams_by_type[stream['stream_type']].append(stream)

    assert len(streams_by_type['audio']) == 2
    assert len(streams_by_type['videocontainer']) == 1
    assert len(streams_by_type['video']) == 1

    assert streams_by_type['audio'][0]['mimetype'] == 'audio/flac'
    assert streams_by_type['videocontainer'][0]['mimetype'] == \
        'video/x-matroska'
    assert streams_by_type['video'][0]['mimetype'] == 'video/x-ffv'

    # V2: ditto
    file_char_ext = file_v2_http_mock.last_request.json()[
        'file_characteristics_extension'
    ]

    # Four different streams found
    assert {'0', '1', '2', '3'} == set(file_char_ext['streams'].keys())

    streams_by_type = defaultdict(list)
    for stream in file_char_ext['streams'].values():
        streams_by_type[stream['stream_type']].append(stream)

    assert len(streams_by_type['audio']) == 2
    assert len(streams_by_type['videocontainer']) == 1
    assert len(streams_by_type['video']) == 1

    assert streams_by_type['audio'][0]['mimetype'] == 'audio/flac'
    assert streams_by_type['videocontainer'][0]['mimetype'] == \
        'video/x-matroska'
    assert streams_by_type['video'][0]['mimetype'] == 'video/x-ffv'


def test_generate_metadata_unrecognized(config, requests_mock, tmp_path):
    """Test metadata generation for unrecognized file.

    File scraper does not recognize for example empty files. Metadata
    generation should raise error if file type is (:unav).

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # create mocked dataset in Metax
    tests.utils.add_metax_v2_dataset(
        requests_mock,
        files=[tests.metax_data.files.BASE_FILE]
    )

    # Create empty file to temporary directory
    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text("")

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier', tmp_path, config)

    assert str(exception_info.value) == 'File format was not recognized'
    assert exception_info.value.files == ['pid:urn:identifier']


def test_generate_metadata_predefined(
        config, file_v3_mock, requests_mock, tmp_path
):
    """Test generate_metadata.

    Tests metadata generation for files that already have some
    file_characteristics defined. File characteristics should not be
    overwritten, but missing information should be added.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_metadata['file_characteristics'] = {
        'encoding': 'user_defined'
    }
    file_metadata['file_characteristics_extension'] = {
        'streams': {
            '0': {
                'charset': 'user_defined',
                'index': 0,
                'mimetype': 'text/plain',
                'stream_type': 'text',
                'version': '(:unap)'
            }
        }
    }
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file_metadata])
    file_v2_http_mock = _create_file_v2_http_mock(requests_mock)

    # Create text file in temporary directory
    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text('foo')

    generate_metadata('dataset_identifier', tmp_path, config)

    # V3: Verify file characteristics were sent to Metax
    assert len(file_v3_mock.mock_calls) == 1
    call = file_v3_mock.mock_calls[0]

    file_metadata = call.args[2]
    assert file_metadata["characteristics"] == {
        # missing keys are added
        'file_format_version': {'file_format': 'text/plain'},
        # user defined value is not overwritten
        'encoding': 'user_defined',
    }
    assert file_metadata["characteristics_extension"]["streams"] == {
        0: {
            'charset': 'user_defined',
            'index': 0,
            'mimetype': 'text/plain',
            'stream_type': 'text',
            'version': '(:unap)'
        }
    }

    # V2: ditto
    json = file_v2_http_mock.last_request.json()
    assert json["file_characteristics"] == {
        # missing keys are added
        'file_format': 'text/plain',
        # user defined value is not overwritten
        'encoding': 'user_defined',

    }
    assert json["file_characteristics_extension"]["streams"] == {
        '0': {
            'charset': 'user_defined',
            'index': 0,
            'mimetype': 'text/plain',
            'stream_type': 'text',
            'version': '(:unap)'
        }
    }


@pytest.mark.parametrize(
    (
        "predefined_file_characteristics",
        "expected_v3_file_characteristics",
        "expected_v2_file_characteristics"
    ),
    [
        (
            # User has predefined all parameters. Predefined parameters
            # should not change
            {
                "file_format": "text/csv",
                "encoding": "UTF-8",
                "csv_delimiter": "x",
                "csv_record_separator": "y",
                "csv_quoting_char": "z"
            },
            {
                "file_format_version": {"file_format": "text/csv"},
                "encoding": "UTF-8",
                "csv_delimiter": "x",
                "csv_record_separator": "y",
                "csv_quoting_char": "z"
            },
            {
                "file_format": "text/csv",
                "encoding": "UTF-8",
                "csv_delimiter": "x",
                "csv_record_separator": "y",
                "csv_quoting_char": "z",
            }
        ),
        (
            # User has predefined only file_format. File-scraper should
            # detect the parameter automatically
            {
                "file_format": "text/csv"
            },
            {
                "file_format_version": {"file_format": "text/csv"},
                "encoding": "UTF-8",
                "csv_delimiter": ";",
                "csv_record_separator": "\r\n",
                "csv_quoting_char": "'"
            },
            {
                "file_format": "text/csv",
                "encoding": "UTF-8",
                "csv_delimiter": ";",
                "csv_record_separator": "\r\n",
                "csv_quoting_char": "'",
            }
        ),
        (
            # User has predefined file_format as plain text. CSV
            # specific parameter should not be added.
            {
                "file_format": "text/plain"
            },
            {
                "file_format_version": {"file_format": "text/plain"},
                "encoding": "UTF-8",
            },
            {
                "file_format": "text/plain",
                "encoding": "UTF-8",
            }
        )
    ]
)
def test_generate_metadata_csv(
        config, file_v3_mock, requests_mock, tmp_path,
        predefined_file_characteristics,
        expected_v3_file_characteristics,
        expected_v2_file_characteristics):
    """Test generate metadata.

    Tests metadata generation for a CSV file.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    :param predefined_file_characteristics: File characteristics that
        have been defined before metadata generation
    :param expected_file_characteristics: File characteristics that
        should be posted to Metax when metadata is generated
    """
    file = copy.deepcopy(tests.metax_data.files.CSV_FILE)
    del file["file_characteristics"]
    del file["file_characteristics_extension"]
    file["file_characteristics"] = predefined_file_characteristics
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file])
    file_v2_http_mock = requests_mock.patch(
        "/rest/v2/files/pid:urn:identifier_csv",
        json={}
    )

    # Create text file in temporary directory
    tmp_file_path = tmp_path / 'path/to/file.csv'
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/text_csv.csv", tmp_file_path)

    generate_metadata('dataset_identifier', tmp_path, config)

    # V3: verify the file characteristics that were sent to Metax
    assert len(file_v3_mock.mock_calls) == 1
    call = file_v3_mock.mock_calls[0]

    tech_metadata = call.args[2]

    file_characteristics = tech_metadata['characteristics']
    assert file_characteristics == expected_v3_file_characteristics

    # The file_characteristics_extension should contain same metadata as
    # file_characteristics
    stream = tech_metadata['characteristics_extension']["streams"][0]
    assert file_characteristics["file_format_version"]["file_format"] \
        == stream["mimetype"]
    assert file_characteristics.get("csv_delimiter") == stream.get("delimiter")
    assert file_characteristics.get("csv_record_separator") \
        == stream.get("separator")
    assert file_characteristics.get("csv_quoting_char") \
        == stream.get("quotechar")

    # V2: ditto
    tech_metadata = file_v2_http_mock.last_request.json()

    file_characteristics = tech_metadata['file_characteristics']
    assert file_characteristics == expected_v2_file_characteristics

    # The file_characteristics_extension should contain same metadata as
    # file_characteristics
    stream = tech_metadata['file_characteristics_extension']["streams"]["0"]
    assert file_characteristics["file_format"] == stream["mimetype"]
    assert file_characteristics.get("csv_delimiter") == stream.get("delimiter")
    assert file_characteristics.get("csv_record_separator") \
        == stream.get("separator")
    assert file_characteristics.get("csv_quoting_char") \
        == stream.get("quotechar")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("file_format", "image/tiff"),
        ("format_version", "foo"),
        ("csv_record_separator", "foo"),
        ("csv_delimiter", "foo"),
        ("csv_quoting_char", "foo"),
        # NOTE: file-scraper does not ignore user defined encoding even
        # if it does not make any sense. So the following test case
        # would fail:
        # ("encoding": "foo")
    ]
)
def test_overwriting_user_defined_metadata(config, requests_mock, tmp_path,
                                           key, value):
    """Test that use defined metadata is not overwritten.

    Exception should be raised if metadata generated by file-scraper
    does not match the pre-defined metadata.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    :param key: key to be modified in file_characteristics
    :param value: value to for key
    """
    file = copy.deepcopy(tests.metax_data.files.TXT_FILE)
    file["file_characteristics"][key] = value
    tests.utils.add_metax_v2_dataset(requests_mock, files=[file])

    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text("foo")

    with pytest.raises(InvalidFileMetadataError) as exception_info:
        generate_metadata('dataset_identifier', tmp_path, config)

    assert str(exception_info.value) \
        == f"File scraper detects a different {key}"
    assert exception_info.value.files == ["pid:urn:identifier"]


def test_generate_metadata_dataset_not_found(config, requests_mock, tmp_path):
    """Test metadatageneration for dataset that does not exist.

    DatasetNotAvailableError should be raised.

    :param config: Configuration file
    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    """
    requests_mock.get("/rest/v2/datasets/foobar/files", status_code=404)

    expected_error = 'Dataset not found'
    with pytest.raises(DatasetNotAvailableError, match=expected_error):
        generate_metadata('foobar', tmp_path, config)


def test_generate_metadata_httperror(config, requests_mock, tmp_path):
    """Test metadata generation when Metax fails.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    tests.utils.add_metax_v2_dataset(requests_mock)
    requests_mock.get(
        "/rest/v2/datasets/dataset_identifier/files",
        status_code=500,
        reason="Fake error"
    )

    expected_error = "500 Server Error: Fake error"
    with pytest.raises(HTTPError, match=expected_error):
        generate_metadata('dataset_identifier', tmp_path, config)
