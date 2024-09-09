"""Tests for :mod:`siptools_research.metadata_generator` module."""
import copy
from collections import defaultdict
from pathlib import Path
import shutil

import pytest
from metax_access import DatasetNotAvailableError
from requests.exceptions import HTTPError

import tests.metax_data.datasets
import tests.metax_data.files
import tests.utils
from siptools_research.exceptions import InvalidFileError
from siptools_research.metadata_generator import generate_metadata


@pytest.mark.parametrize(
    [
        'path',
        'expected_file_characteristics',
        'expected_stream_type'
    ],
    [
        # Text file should have encoding, but not format_version
        (
            'tests/data/sample_files/text_plain_UTF-8',
            {
                'file_format': 'text/plain',
                'encoding': 'UTF-8',
            },
            'text'
        ),
        (
            'tests/data/sample_files/image_png.png',
            {
                'file_format': 'image/png',
                'format_version': '1.2'
            },
            'image'
        ),
        (
            'tests/data/sample_files/image_tiff_large.tif',
            {
                'file_format': 'image/tiff',
                'format_version': '6.0'
            },
            'image'
        ),
        # WAV file should not have container stream according to DPS
        # specs
        (
            'tests/data/sample_files/audio_x-wav.wav',
            {
                'file_format': 'audio/x-wav'
            },
            'audio'
        ),
        # The first stream of matroska file should be container
        (
            'tests/data/sample_files/video_ffv1.mkv',
            {
                'file_format': 'video/x-matroska',
                'format_version': '4'
            },
            'videocontainer'
        ),
        # Ensure that problematic ODF formats (especially ODF Formula)
        # are detected correctly
        (
            'tests/data/sample_files/opendocument_text.odt',
            {
                'file_format': 'application/vnd.oasis.opendocument.text',
                'format_version': '1.2'
            },
            'binary'
        ),
        (
            'tests/data/sample_files/opendocument_formula.odf',
            {
                'file_format': 'application/vnd.oasis.opendocument.formula',
                'format_version': '1.2'
            },
            'binary'
        )
    ]
)
def test_generate_metadata(requests_mock,
                           path,
                           expected_file_characteristics,
                           expected_stream_type,
                           testpath):
    """Test metadata generation.

    Generates metadata for a dataset that contains one file, and checks
    that correct file characteristics and file_characteristics_extension
    are sent to Metax.

    :param requests_mock: Mocker object
    :param path: path to the file for which the metadata is created
    :param expected_file_characteristics: expected file_characteristics
    :param expected_stream_type: expected type of first stream in
                                 file_characteristics_extension
    :param testpath: Temporary directory
    :returns: ``None``
    """
    # create mocked dataset in Metax
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_path = Path('/path/to') / Path(path).name
    file_metadata['file_path'] = str(file_path)
    tests.utils.add_metax_dataset(requests_mock,
                                  files=[file_metadata])
    file_metadata_patch = requests_mock.patch(
        "https://metaksi/rest/v2/files/pid:urn:identifier",
        json={}
    )

    # Copy the file to temporary directory
    tmp_file_path = testpath / file_path.relative_to('/')
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy(path, tmp_file_path)

    # Generate metadata
    generate_metadata('dataset_identifier',
                      root_directory=testpath,
                      config=tests.conftest.UNIT_TEST_CONFIG_FILE)

    # verify the file characteristics that were sent to Metax
    file_characteristics \
        = file_metadata_patch.last_request.json()['file_characteristics']
    assert file_characteristics == expected_file_characteristics

    # Check that at least the mimetype and stream type of the are
    # correct in the first stream of file_characteristics_extension
    file_char_ext = file_metadata_patch.last_request.json()[
        'file_characteristics_extension'
    ]
    assert file_char_ext['streams']['0']['mimetype'] \
        == expected_file_characteristics['file_format']
    assert file_char_ext['streams']['0']['stream_type'] == expected_stream_type


def test_generate_metadata_video_streams(requests_mock, testpath):
    """Test metadata generation for a video file.

    Generates file characteristics for a video file with multiple
    streams.

    :param requests_mock: HTTP request mocker
    :param testpath: Temporary directory
    """
    tests.utils.add_metax_dataset(
        requests_mock,
        files=[tests.metax_data.files.BASE_FILE])
    file_metadata_patch = requests_mock.patch(
        "https://metaksi/rest/v2/files/pid:urn:identifier",
        json={}
    )

    tmp_file_path = testpath / "path/to/file"
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/video_ffv1.mkv', tmp_file_path)

    generate_metadata(
        'dataset_identifier', testpath, tests.conftest.UNIT_TEST_CONFIG_FILE
    )

    file_char_ext = file_metadata_patch.last_request.json()[
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


def test_generate_metadata_unrecognized(requests_mock, testpath):
    """Test metadata generation for unrecognized file.

    File scraper does not recognize for example empty files. Metadata
    generation should raise error if file type is (:unav).

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    # create mocked dataset in Metax
    tests.utils.add_metax_dataset(requests_mock,
                                  files=[tests.metax_data.files.BASE_FILE])

    # Create empty file to temporary directory
    tmp_file_path = testpath / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text("")

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier',
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) == 'File format was not recognized'
    assert exception_info.value.files == ['pid:urn:identifier']


def test_generate_metadata_predefined(requests_mock, testpath):
    """Test generate_metadata.

    Tests metadata generation for files that already have some
    file_characteristics defined. File characteristics should not be
    overwritten, but missing information should be added.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    file_metadata = copy.deepcopy(tests.metax_data.files.BASE_FILE)
    file_metadata['file_characteristics'] = {
        'encoding': 'user_defined',
        'dummy_key': 'dummy_value'

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
    tests.utils.add_metax_dataset(requests_mock, files=[file_metadata])
    patch_request = requests_mock.patch(
        "https://metaksi/rest/v2/files/pid:urn:identifier",
        json={}
    )

    # Create text file in temporary directory
    tmp_file_path = testpath / 'path/to/file'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text('foo')

    generate_metadata('dataset_identifier',
                      testpath,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    # verify the file characteristics that were sent to Metax
    assert patch_request.last_request.json() == {
        'file_characteristics': {
            # missing keys are added
            'file_format': 'text/plain',
            # user defined value is not overwritten
            'encoding': 'user_defined',
            # additional keys are copied
            'dummy_key': 'dummy_value',
        },
        'file_characteristics_extension': {
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
    }


def test_generate_metadata_csv(requests_mock, testpath):
    """Test generate metadata.

    Tests addml metadata generation for a CSV file. Generates file
    characteristics metadata that is later used to generate an ADDML
    document.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    tests.utils.add_metax_dataset(requests_mock,
                                  files=[tests.metax_data.files.CSV_FILE])

    # Create text file in temporary directory
    tmp_file_path = testpath / 'path/to/file.csv'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text('foo')

    generate_metadata('dataset_identifier',
                      testpath,
                      tests.conftest.UNIT_TEST_CONFIG_FILE)

    tech_metadata = requests_mock.last_request.json()
    file_characteristics = tech_metadata['file_characteristics']
    file_char_ext = tech_metadata['file_characteristics_extension']

    assert file_characteristics['file_format'] == 'text/csv'
    assert file_characteristics['csv_delimiter'] == ';'

    assert file_char_ext['streams']['0']['mimetype'] == 'text/csv'


def test_generate_metadata_wrong_type(requests_mock, testpath):
    """Test generate metadata for a file which type is falsely defined.

    User has defined the type of the CSV file as image/tiff and
    scraper returns CSV as a type. An error is thrown.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    csv_file = copy.deepcopy(tests.metax_data.files.CSV_FILE)
    csv_file["file_characteristics"]["file_format"] = "image/tiff"
    tests.utils.add_metax_dataset(requests_mock,
                                  files=[csv_file])

    # Create text file in temporary directory
    tmp_file_path = testpath / 'path/to/file.csv'
    tmp_file_path.parent.mkdir(parents=True)
    tmp_file_path.write_text('foo')

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier',
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) == 'File scraper detects a different file type'
    assert exception_info.value.files == ['pid:urn:identifier_csv']


def test_generate_metadata_wrong_version(requests_mock, testpath):
    """Test generating metadata for a file with wrong version defined.

    :param requests_mock: Mocker object
    :param testpath: Temporary directory
    """
    file = copy.deepcopy(tests.metax_data.files.TIFF_FILE)
    # Change the predefined version. Scraper should detect the correct
    # version, which is 6.0
    file["file_characteristics"]["format_version"] = "9.9999999"
    tests.utils.add_metax_dataset(requests_mock, files=[file])

    tmp_file_path = testpath / 'path/to/file.tiff'
    tmp_file_path.parent.mkdir(parents=True)
    shutil.copy('tests/data/sample_files/image_tiff_large.tif', tmp_file_path)

    with pytest.raises(InvalidFileError) as exception_info:
        generate_metadata('dataset_identifier',
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)

    assert str(exception_info.value) \
        == 'File scraper detects a different file version'
    assert exception_info.value.files == ['pid:urn:identifier_tiff']


def test_generate_metadata_dataset_not_found(requests_mock, testpath):
    """Test metadatageneration for dataset that does not exist.

    DatasetNotAvailableError should be raised.

    :param monkeypatch: Monkeypatch object
    :param requests_mock: Mocker object
    """
    requests_mock.get('https://metaksi/rest/v2/datasets/foobar/files',
                      status_code=404)

    expected_error = 'Dataset not found'
    with pytest.raises(DatasetNotAvailableError, match=expected_error):
        generate_metadata('foobar',
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)


def test_generate_metadata_httperror(requests_mock, testpath):
    """Test metadata generation when Metax fails.

    :param requests_mock: Mocker object
    """
    tests.utils.add_metax_dataset(requests_mock)
    requests_mock.get(
        'https://metaksi/rest/v2/datasets/dataset_identifier/files',
        status_code=500,
        reason='Fake error'
    )

    expected_error = "500 Server Error: Fake error"
    with pytest.raises(HTTPError, match=expected_error):
        generate_metadata('dataset_identifier',
                          testpath,
                          tests.conftest.UNIT_TEST_CONFIG_FILE)
