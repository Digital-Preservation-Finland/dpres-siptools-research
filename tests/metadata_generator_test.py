"""Tests for :mod:`siptools_research.metadata_generator` module."""
import copy
import shutil
from collections import defaultdict
from pathlib import Path

import pytest
from metax_access import DatasetNotAvailableError
from requests.exceptions import HTTPError

import tests.metax_data.reference_data
import tests.utils
from siptools_research.exceptions import (
    InvalidFileError,
    InvalidFileMetadataError,
)
from siptools_research.metadata_generator import generate_metadata
from tests.metax_data.files import BASE_FILE, TXT_FILE


def test_generate_metadata(config, requests_mock, tmp_path):
    """Test metadata generation.

    Generates metadata for text file. Checks that expected requests are
    sent to Metax.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param tmp_path: Temporary path
    """
    # create mocked dataset in Metax
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata["pathname"] = "textfile"
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)
    patch_characteristics_mock \
        = requests_mock.patch("/v3/files/pid:urn:identifier/characteristics",
                              json={})
    patch_file_mock \
        = requests_mock.patch("/v3/files/pid:urn:identifier", json={})

    # Create a text file in temporary directory
    tmp_file_path = tmp_path / "textfile"
    tmp_file_path.write_text("foo")

    # Generate metadata
    generate_metadata("dataset_identifier",
                      root_directory=tmp_path,
                      config=config)

    # Verify the file characteristics that were sent to Metax
    file_characteristics = patch_characteristics_mock.last_request.json()
    assert file_characteristics == {
        "file_format_version": {
            "url": "url-for-txt"
        },
        "encoding": "UTF-8"
    }

    # Verify the file characteristics extension that was sent to Metax
    json = patch_file_mock.last_request.json()
    file_char_ext = json["characteristics_extension"]
    assert file_char_ext["streams"] == {
        '0': {
            "charset": "UTF-8",
            "index": 0,
            "mimetype": "text/plain",
            "stream_type": "text",
            "version": "(:unap)"
        }
    }
    # It does not make sense to validate "info", but at least it
    # should not be empty
    assert file_char_ext["info"]
    assert file_char_ext["mimetype"] == "text/plain"
    assert file_char_ext["version"] ==  "(:unap)"
    assert file_char_ext["grade"] == "fi-dpres-recommended-file-format"


@pytest.mark.parametrize(
    (
        "path",
        "expected_url",
        "expected_file_format",
        "expected_format_version",
        "expected_encoding",
        "expected_stream_type"
    ),
    [
        # Text file should have encoding, but not format_version
        (
            "tests/data/sample_files/text_plain_UTF-8",
            "url-for-txt",
            "text/plain",
            None,
            "UTF-8",
            "text",
        ),
        (
            "tests/data/sample_files/image_png.png",
            "url-for-png",
            "image/png",
            "1.2",
            None,
            "image",
        ),
        (
            "tests/data/sample_files/image_tiff_large.tif",
            "url-for-tif",
            "image/tiff",
            "6.0",
            None,
            "image",
        ),
        # WAV file should not have container stream according to DPS
        # specs
        (
            "tests/data/sample_files/audio_x-wav.wav",
            "url-for-wav",
            "audio/x-wav",
            None,
            None,
            "audio",
        ),
        # The first stream of matroska file should be container
        (
            "tests/data/sample_files/video_ffv1.mkv",
            "url-for-mkv",
            "video/x-matroska",
            "4",
            None,
            "videocontainer",
        ),
        # Ensure that problematic ODF formats (especially ODF Formula)
        # are detected correctly
        (
            "tests/data/sample_files/opendocument_text.odt",
            "url-for-odt",
            "application/vnd.oasis.opendocument.text",
            "1.2",
            None,
            "binary",
        ),
        (
            "tests/data/sample_files/opendocument_formula.odf",
            "url-for-odf",
            "application/vnd.oasis.opendocument.formula",
            "1.2",
            None,
            "binary",
        )
    ]
)
def test_file_format_detection(config, requests_mock, path, expected_url,
                               expected_file_format, expected_format_version,
                               expected_encoding, expected_stream_type,
                               tmp_path):
    """Test file format detection.

    Generates metadata for a dataset that contains one file, and checks
    that file format is detected correctly.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param path: path to the file for which the metadata is created
    :param expected_url: Expected file_format_version url
    :param expected_file_format: Expected file format
    :param expected_format_version: Expected file format version
    :param expected_encoding: Expected file encoding
    :param expected_stream_type: Expected type of first stream in
                                 file_characteristics_extension
    :param tmp_path: Temporary directory
    """
    # create mocked dataset in Metax
    file_metadata = copy.deepcopy(BASE_FILE)
    file_path = Path('/path/to') / Path(path).name
    file_metadata['pathname'] = str(file_path)
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    patch_characteristics_mock = requests_mock.patch(
        "/v3/files/pid:urn:identifier/characteristics",
        json={}
    )
    patch_file_mock = requests_mock.patch("/v3/files/pid:urn:identifier",
                                          json={})
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    # Copy the file to temporary directory
    tmp_file_path = tmp_path / file_path.relative_to('/')
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy(path, tmp_file_path)

    # Generate metadata
    generate_metadata("dataset_identifier",
                      root_directory=tmp_path,
                      config=config)

    # Check that expected file characteristics were sent to Metax
    characteristics = patch_characteristics_mock.last_request.json()
    assert characteristics["file_format_version"]["url"] \
        == expected_url

    # Check that expected file characteristics extension was sent to
    # Metax
    stream = (patch_file_mock.last_request.json()
              ["characteristics_extension"]["streams"]["0"])
    assert stream["mimetype"] == expected_file_format
    assert stream["stream_type"] == expected_stream_type


def test_generate_metadata_video_streams(config, requests_mock, tmp_path):
    """Test metadata generation for a video file.

    Generates file characteristics for a video file with multiple
    streams.

    :param config: Configuration file
    :param requests_mock: HTTP request mocker
    :param tmp_path: Temporary directory
    """
    # Mock Metax
    tests.utils.add_metax_dataset(requests_mock, files=[BASE_FILE])
    requests_mock.patch("/v3/files/pid:urn:identifier/characteristics",
                        json={})
    file_http_mock = requests_mock.patch("/v3/files/pid:urn:identifier",
                                         json={})
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    # Copy video file to temporary path
    tmp_file_path = tmp_path / "path/to/file"
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/video_ffv1.mkv', tmp_file_path)

    generate_metadata('dataset_identifier', tmp_path, config)

    file_char_ext = file_http_mock.last_request.json()[
        'characteristics_extension'
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
    tests.utils.add_metax_dataset(requests_mock, files=[BASE_FILE])
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    # Create empty file to temporary directory
    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text("")

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier', tmp_path, config)

    assert str(exception_info.value) == 'File format was not recognized'
    assert exception_info.value.files == ['pid:urn:identifier']


def test_generate_metadata_predefined(config, requests_mock, tmp_path):
    """Test generate_metadata.

    Tests metadata generation for files that already have some
    file_characteristics defined. File characteristics should not be
    overwritten, but missing information should be added.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    """
    # Mock Metax
    file_metadata = copy.deepcopy(BASE_FILE)
    file_metadata["characteristics"]["encoding"] = "user_defined"
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    patch_file_mock = requests_mock.patch("/v3/files/pid:urn:identifier",
                                         json={})
    patch_characteristics_mock \
        = requests_mock.patch("/v3/files/pid:urn:identifier/characteristics",
                              json={})
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    # Create text file in temporary directory
    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text('foo')

    generate_metadata('dataset_identifier', tmp_path, config)

    # Check that expected metadata was sen to Metax
    json = patch_characteristics_mock.last_request.json()
    assert json == {
        # missing keys are added
        "file_format_version":{
            "url": "url-for-txt",
        },
        # user defined value is not overwritten
        "encoding": "user_defined",
    }

    # Check that user defined value is copied also to file
    # characteristics extension
    json = patch_file_mock.last_request.json()
    assert json["characteristics_extension"]["streams"] == {
        '0': {
            'charset': 'user_defined',
            'index': 0,
            'mimetype': 'text/plain',
            'stream_type': 'text',
            'version': '(:unap)'
        }
    }


@pytest.mark.parametrize(
    ( "predefined_file_characteristics", "expected_file_characteristics"),
    [
        (
            # User has predefined all parameters. Predefined parameters
            # should not change
            {
                "file_format_version": {
                    "file_format": "text/csv",
                    "format_version": None
                },
                "encoding": "UTF-8",
                "csv_delimiter": "x",
                "csv_record_separator": "LF",
                "csv_quoting_char": "z"
            },
            {
                "file_format_version": {"url": "url-for-csv"},
                "encoding": "UTF-8",
                "csv_delimiter": "x",
                "csv_record_separator": "LF",
                "csv_quoting_char": "z"
            },
        ),
        (
            # User has predefined only file_format. File-scraper should
            # detect the csv parameters automatically
            {
                "file_format_version": {
                    "file_format": "text/csv",
                    "format_version": None
                },
            },
            {
                "file_format_version": {"url": "url-for-csv"},
                "encoding": "UTF-8",
                "csv_delimiter": ";",
                "csv_record_separator": "CRLF",
                "csv_quoting_char": "'"
            },
        ),
        (
            # User has predefined file_format as plain text. CSV
            # specific parameters should not be added.
            {
                "file_format_version": {
                    "file_format": "text/plain",
                    "format_version": None
                },
            },
            {
                "file_format_version": {"url": "url-for-txt"},
                "encoding": "UTF-8",
            },
        )
    ]
)
def test_generate_metadata_csv(
        config, requests_mock, tmp_path,
        predefined_file_characteristics,
        expected_file_characteristics,
):
    """Test generate metadata.

    Tests metadata generation for a CSV file.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    :param predefined_file_characteristics: File characteristics that
        have been defined before metadata generation in Metax
    :param expected_file_characteristics: File characteristics that
        should be posted to Metax when metadata is generated
    """
    # Mock Metax
    file = copy.deepcopy(BASE_FILE)
    file["characteristics"].update(predefined_file_characteristics)
    tests.utils.add_metax_dataset(requests_mock, files=[file])
    patch_file_mock = requests_mock.patch(
        "/v3/files/pid:urn:identifier",
        json={}
    )
    patch_characteristics_mock = requests_mock.patch(
        "/v3/files/pid:urn:identifier/characteristics",
        json={}
    )
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    # Create text file in temporary directory
    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy("tests/data/sample_files/text_csv.csv", tmp_file_path)

    generate_metadata('dataset_identifier', tmp_path, config)

    # Check that expected metadata was sent to Metax
    file_characteristics \
        = patch_characteristics_mock.last_request.json()
    assert file_characteristics == expected_file_characteristics

    # The file_characteristics_extension should contain same
    # metadata as file_characteristics
    stream = (patch_file_mock.last_request.json()
              ['characteristics_extension']["streams"]["0"])
    assert file_characteristics.get("csv_delimiter") \
        == stream.get("delimiter")

    assert file_characteristics.get("csv_quoting_char") \
        == stream.get("quotechar")

    # The 'csv_record_separator'
    # in 'file_characteristics' (NOT 'file_characteristics_extension')
    # has to adhere to Metax V3's enum
    char_separator = file_characteristics.get("csv_record_separator")
    if char_separator:
        char_separator = char_separator.replace("CR", "\r").replace("LF", "\n")

    assert char_separator == stream.get("separator")


@pytest.mark.parametrize(
    (
        "keys",
        "predefined_value",
        "detected_value",
        "filepath"),
    [
        (
            # TODO: This is a special case. See TPASPKT-1418.
            ["file_format_version", "file_format"],
            "image/tiff",
            "application/vnd.oasis.opendocument.text",
            "tests/data/sample_files/opendocument_text.odt",
        ),
        (
            # Metax does not allow "foo" as format_version so this not a
            # realistic test case.
            ["file_format_version", "format_version"],
            "foo",
            "(:unap)",
            "tests/data/sample_files/text_plain_UTF-8",
        ),
        (
            ["csv_record_separator"],
            "CR",
            None,
            "tests/data/sample_files/text_plain_UTF-8",
        ),
        (
            ["csv_delimiter"],
            "foo",
            None,
            "tests/data/sample_files/text_plain_UTF-8",
        ),
        (
            ["csv_quoting_char"],
            "foo",
            None,
            "tests/data/sample_files/text_plain_UTF-8",
        ),
        # NOTE: file-scraper does not ignore user defined encoding even
        # if it does not make any sense. So the following test case
        # would fail:
        # (
        #     "encoding",
        #     "foo",
        #     None,
        #     "tests/data/sample_files/text_plain_UTF-8"
        # )
    ]
)
# TODO: Why does file-scraper generate metadata that conflicts with
# predefined metadata? Maybe this problem should be resolved in
# file-scraper, so this test would not be necessary. See TPASPKT-1418
# for more information.
def test_overwriting_user_defined_metadata(config, requests_mock, tmp_path,
                                           keys, predefined_value,
                                           detected_value, filepath):
    """Test that user defined metadata is not overwritten.

    Exception should be raised if metadata generated by file-scraper
    does not match the pre-defined metadata.

    :param config: Configuration file
    :param requests_mock: Mocker object
    :param tmp_path: Temporary directory
    :param keys: key to be modified in file characteristics
    :param value: value for the key
    :param detected_value: The conflicting value detected by Scraper
    :param filepath: Sample file to be scraped
    """
    # Mock Metax
    file = copy.deepcopy(TXT_FILE)

    # Update the predefined value in file characteristics
    obj=file["characteristics"]
    for key in keys[:-1]:
        obj=obj[key]
    obj[keys[-1]] = predefined_value

    tests.utils.add_metax_dataset(requests_mock, files=[file])
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    tmp_file_path = tmp_path / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy(filepath, tmp_file_path)

    with pytest.raises(InvalidFileMetadataError) as exception_info:
        generate_metadata('dataset_identifier', tmp_path, config)

    assert str(exception_info.value)\
        == f"File scraper detects a different {keys[-1]}: {detected_value}"

    assert exception_info.value.files == ["pid:urn:identifier"]


def test_generate_metadata_dataset_not_found(config, requests_mock, tmp_path):
    """Test metadata generation for dataset that does not exist.

    DatasetNotAvailableError should be raised.

    :param config: Configuration file
    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    """
    # Mock Metax
    requests_mock.get("/v3/datasets/foobar/files", status_code=404)
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    expected_error = "Dataset not found"
    with pytest.raises(DatasetNotAvailableError, match=expected_error):
        generate_metadata("foobar", tmp_path, config)


def test_generate_metadata_httperror(config, requests_mock, tmp_path):
    """Test that metadata generation when Metax fails.

    :param config: Configuration file
    :param requests_mock: Mocker object
    """
    # Mock Metax
    requests_mock.get("/v3/datasets/foobar/files",
                      status_code=500,
                      reason="Fake error")
    requests_mock.get("/v3/reference-data/file-format-versions",
                      json=tests.metax_data.reference_data.FILE_FORMAT_VERSIONS)

    expected_error = "500 Server Error: Fake error"
    with pytest.raises(HTTPError, match=expected_error):
        generate_metadata("foobar", tmp_path, config)
