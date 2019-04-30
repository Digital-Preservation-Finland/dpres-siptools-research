"""Module that generates file metadata XML element to be stored into Metax
"""

from abc import ABCMeta, abstractmethod
from siptools.scripts import create_mix
from siptools.scripts import create_addml
from siptools.scripts import create_audiomd


class _XMLMetadata:
    """ Abstract base class for XML metadata generators"""
    __metaclass__ = ABCMeta

    def __init__(self, file_path, file_metadata):
        """
        :param file_path: path of the file in filesystem
        :param file_metadata: metax file metadata as dict
        :returns: ``None``
        """
        self.file_path = file_path
        self.file_metadata = file_metadata

    @abstractmethod
    def create(self):
        """ Abstract method to be implemented by a subclass. Creates a file
        type specific XML metadata.
        :returns: metadata XML element
        """
        pass

    @classmethod
    def is_generator_for(cls, file_format):
        """ Class method to be implemented by a subclass.
        :returns: ``Boolean``: True if this generator generates the XML
            metadata for the given file_format. Otherwise False.
        """
        pass


class _ImageFileXMLMetadata(_XMLMetadata):
    """Class for creating XML metadata for image files."""
    def create(self):
        """Creates a MIX metadata XML element for an image file.
        :returns: MIX XML element
        """
        return create_mix.create_mix_metadata(self.file_path)

    @classmethod
    def is_generator_for(cls, file_format):
        """
        :returns: ``Boolean``: True if provided file_format starts with
            ``image``. Otherwise False.
        """
        return file_format.startswith('image')


class _CSVFileXMLMetadata(_XMLMetadata):
    """Class for creating metadata XML element for CSV files."""
    def create(self):
        """Creates ADDML metadata XML elementfor a CSV file.
        :returns: ADDML metadata XML element
        """
        for attribute in ('csv_delimiter',
                          'csv_has_header',
                          'encoding',
                          'csv_record_separator',
                          'csv_quoting_char'):
            if attribute not in self.file_metadata['file_characteristics']:
                raise Exception('Required attribute "%s" is missing'
                                ' from file characteristics of a '
                                'CSV file.' % attribute)

        return create_addml.create_addml_metadata(
            self.file_path,
            self.file_metadata['file_characteristics']['csv_delimiter'],
            self.file_metadata['file_characteristics']['csv_has_header'],
            self.file_metadata['file_characteristics']['encoding'],
            self.file_metadata['file_characteristics']['csv_record_separator'],
            self.file_metadata['file_characteristics']['csv_quoting_char'],
            flatfile_name=self.file_metadata['file_path']
        )

    @classmethod
    def is_generator_for(cls, file_format):
        """
        :returns: ``Boolean``: True if provided file_format is ``text/csv``.
            Otherwise False.
        """
        return file_format == 'text/csv'


class _AudioXWavFileXMLMetadata(_XMLMetadata):
    """Class for creating XML metadata for audio files."""
    def create(self):
        """Creates and returns the root audioMD XML element.
        :returns: audioMD XML element
        """
        return create_audiomd.create_audiomd_metadata(self.file_path)['0']

    @classmethod
    def is_generator_for(cls, file_format):
        """
        :returns: ``Boolean``: True if provided file_format is ``audio/x-wav``.
            Otherwise False.
        """
        return file_format == 'audio/x-wav'


class XMLMetadataGenerator(object):
    """ Class for generating a file type specific XML metadata. """
    METADATA_GENERATORS = [_ImageFileXMLMetadata, _CSVFileXMLMetadata,
                           _AudioXWavFileXMLMetadata]

    def __init__(self, file_path, file_metadata):
        """
        :param file_path: path of the file in filesystem
        :param file_metadata: metax file metadata as dict
        :returns: ``None``
        """
        self.generator = None
        for generator in self.METADATA_GENERATORS:
            if generator.is_generator_for(file_metadata['file_characteristics']
                                          ['file_format']):
                self.generator = generator(file_path, file_metadata)

    def create(self):
        """Creates and returns file specific XML metadata element or None.
        :returns: metadata XML element or ``None``
        """
        if self.generator is not None:
            return self.generator.create()
        else:
            return None
