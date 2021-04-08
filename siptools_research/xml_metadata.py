"""Module that generates file metadata XML element to be stored into Metax."""
import datetime
import os
import uuid
from abc import ABCMeta, abstractmethod

from mets import amdsec, mdwrap, mets, techmd, xmldata

from siptools.scripts import (create_addml, create_audiomd, create_mix,
                              create_videomd)
from siptools.scripts.create_mix import MixGenerationError
from siptools.xml.mets import METS_MDTYPES, NAMESPACES
from siptools_research.exceptions import (InvalidFileError,
                                          InvalidFileMetadataError)

# XML namespace -> technical metadata dict mapping
TECH_ATTR_TYPES = {
    'http://www.loc.gov/mix/v20': {
        'mdtype': 'NISOIMG',
        'namespace': 'http://www.loc.gov/mix/v20',
        'mdtypeversion': '2.0',
        'othermdtype': None,
        'ref_file': 'create-mix-md-references.jsonl'
    },
    'http://www.arkivverket.no/standarder/addml': {
        'mdtype': 'OTHER',
        'namespace': 'http://www.arkivverket.no/standarder/addml',
        'mdtypeversion': '8.3',
        'othermdtype': 'ADDML',
        'ref_file': 'create-addml-md-references.jsonl'
    },
    'http://www.loc.gov/audioMD/': {
        'mdtype': 'OTHER',
        'namespace': 'http://www.loc.gov/audioMD/',
        'mdtypeversion': '2.0',
        'othermdtype': 'AudioMD',
        'ref_file': 'create-audiomd-md-references.jsonl'
    },
    'http://www.loc.gov/videoMD/': {
        'mdtype': 'OTHER',
        'namespace': 'http://www.loc.gov/videoMD/',
        'mdtypeversion': '2.0',
        'othermdtype': 'VideoMD',
        'ref_file': 'create-videomd-md-references.jsonl'
    },
}


def _kwargs2str(kwargs):
    """Format dict kwargs into a human readable list."""
    keys = sorted(kwargs.keys())
    kwarg_list = " ["

    for i in range(len(keys) - 1):
        kwarg_list += " %s=%s," % (keys[i], kwargs[keys[i]])

    return kwarg_list + " %s=%s ]" % (keys[-1], kwargs[keys[-1]])


class _XMLMetadata:
    """Abstract base class for XML metadata generators."""

    __metaclass__ = ABCMeta

    def __init__(self, file_path, file_metadata):
        """Initialize XML metadata class.

        :param file_path: path of the file in filesystem
        :param file_metadata: metax file metadata as dict
        :returns: ``None``
        """
        self.file_path = file_path
        self.file_metadata = file_metadata

    @abstractmethod
    def create(self):
        """Abstract method to be implemented by a subclass.

        Creates a file type specific XML metadata.

        :returns: metadata XML element
        """

    @property
    def streams(self):
        """Return streams as an integer-indexed dict

        The stream dict returned by file-scraper is serialized into JSON and
        then back into a Python dict. When serialized into JSON, any integer
        keys are implicitly converted into strings.

        Since this means that any integer keys
        will be converted to strings, we restore the original integer keys
        in this property if necessary.
        """
        orig_streams = \
            self.file_metadata["file_characteristics_extension"]["streams"]

        return {
            int(key): value for key, value in orig_streams.items()
        }


    @classmethod
    def is_generator_for(cls, file_metadata):
        """Class method to be implemented by a subclass.

        :returns: ``Boolean``: True if this generator generates the XML
                  metadata for the given file_metadata. Otherwise False.
        """
        raise NotImplementedError


class _ImageFileXMLMetadata(_XMLMetadata):
    """Class for creating XML metadata for image files."""

    def create(self):
        """Create a MIX metadata XML element for an image file.

        :returns: List containing a MIX XML element
        """
        try:
            mix_elem = create_mix.create_mix_metadata(
                self.file_path,
                streams=self.streams
            )
            return [mix_elem]
        except MixGenerationError as error:
            # Clean up file path in original exception message and raise error
            error.filename = os.path.split(error.filename)[1]
            raise InvalidFileError(str(error),
                                   [self.file_metadata['identifier']])

    @classmethod
    def is_generator_for(cls, file_metadata):
        """Check if class is generator for file format.

        :returns: ``Boolean``: True if provided file_characteristics contains
                  at least one ``image`` stream. Otherwise False.
        """
        file_char_ext = file_metadata["file_characteristics_extension"]
        return any(
            stream for stream in file_char_ext["streams"].values()
            if stream["stream_type"] == "image"
        )


class _CSVFileXMLMetadata(_XMLMetadata):
    """Class for creating metadata XML element for CSV files."""

    def create(self):
        """Create ADDML metadata XML elementfor a CSV file.

        :returns: List containing an ADDML metadata XML element
        """
        for attribute in ('csv_delimiter',
                          'csv_has_header',
                          'encoding',
                          'csv_record_separator',
                          'csv_quoting_char'):
            if attribute not in self.file_metadata['file_characteristics']:
                raise InvalidFileMetadataError(
                    'Required attribute "%s" is missing from file '
                    'characteristics of a CSV file.' % attribute,
                    [self.file_metadata['identifier']]
                )

        addml_elem = create_addml.create_addml_metadata(
            csv_file=self.file_path,
            delimiter=self.file_metadata['file_characteristics'][
                'csv_delimiter'],
            isheader=self.file_metadata['file_characteristics'][
                'csv_has_header'],
            charset=self.file_metadata['file_characteristics']['encoding'],
            record_separator=self.file_metadata['file_characteristics'][
                'csv_record_separator'],
            quoting_char=self.file_metadata['file_characteristics'][
                'csv_quoting_char'],
            flatfile_name=self.file_metadata['file_path']
        )

        return [addml_elem]

    @classmethod
    def is_generator_for(cls, file_metadata):
        """Check if class is generator for file format.

        :returns: ``Boolean``: True if provided file format is ``text/csv``.
                  Otherwise False.
        """
        return file_metadata['file_characteristics']['file_format'] \
            == 'text/csv'


class _AudioFileXMLMetadata(_XMLMetadata):
    """Class for creating XML metadata for audio files."""

    def create(self):
        """Create the root audioMD XML element.

        :returns: List of audioMD XML elements
        """
        audiomd = create_audiomd.create_audiomd_metadata(
            self.file_path,
            streams=self.streams
        )
        if not audiomd:
            raise InvalidFileError("Audio file has no audio streams.",
                                   [self.file_metadata['identifier']])

        return list(audiomd.values())

    @classmethod
    def is_generator_for(cls, file_metadata):
        """Check if class is generator for file format.

        :returns: ``Boolean``: True if provided file_characteristics
                  contains at least one audio stream.
                  Otherwise False.
        """
        file_char_ext = file_metadata["file_characteristics_extension"]
        return any(
            stream for stream in file_char_ext["streams"].values()
            if stream["stream_type"] == "audio"
        )


class _VideoFileXMLMetadata(_XMLMetadata):
    """Class for creating XML metadata for video files."""

    def create(self):
        """Create the root audioMD XML element.

        :returns: List of videoMD XML elements
        """
        videomd = create_videomd.create_videomd_metadata(
            self.file_path,
            streams=self.streams
        )
        if not videomd:
            raise InvalidFileError("Video file has no video streams.",
                                   [self.file_metadata['identifier']])

        return list(videomd.values())

    @classmethod
    def is_generator_for(cls, file_metadata):
        """Check if class is generator for file format.

        :returns: ``Boolean``: True if provided file_characteristics
                  contains at least one video stream.
                  Otherwise False.
        """
        file_char_ext = file_metadata["file_characteristics_extension"]
        return any(
            stream for stream in file_char_ext["streams"].values()
            if stream["stream_type"] == "video"
        )


class XMLMetadataGenerator(object):
    """Class for generating a file type specific XML metadata."""

    METADATA_GENERATORS = [_ImageFileXMLMetadata, _CSVFileXMLMetadata,
                           _AudioFileXMLMetadata, _VideoFileXMLMetadata]

    def __init__(self, file_path, file_metadata):
        """Initialize XML metadata generator.

        :param file_path: path of the file in filesystem
        :param file_metadata: metax file metadata as dict
        :returns: ``None``
        """
        self.generators = []
        for generator in self.METADATA_GENERATORS:
            if generator.is_generator_for(file_metadata):
                self.generators.append(generator(file_path, file_metadata))

    def create(self):
        """Create file specific XML metadata element or None.

        :returns: metadata XML element or ``None``
        """
        results = []
        for generator in self.generators:
            results += generator.create()

        return results
