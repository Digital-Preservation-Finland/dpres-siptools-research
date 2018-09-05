# encoding=utf8
"""Metax interface class."""

import iso639
import lxml.etree
import requests
from requests.auth import HTTPBasicAuth
from siptools_research.config import Configuration

DS_STATE_INITIALIZED = 0
DS_STATE_PROPOSED_FOR_DIGITAL_PRESERVATION = 10
DS_STATE_TECHNICAL_METADATA_GENERATED = 20
DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED = 30
DS_STATE_INVALID_METADATA = 40
DS_STATE_METADATA_VALIDATION_FAILED = 50
DS_STATE_VALIDATED_METADATA_UPDATED = 60
DS_STATE_VALID_METADATA = 70
DS_STATE_METADATA_CONFIRMED = 75
DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION = 80
DS_STATE_IN_PACKAGING_SERVICE = 90
DS_STATE_PACKAGING_FAILED = 100
DS_STATE_SIP_SENT_TO_INGESTION_IN_DPRES_SERVICE = 110
DS_STATE_IN_DIGITAL_PRESERVATION = 120
DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE = 130
DS_STATE_IN_DISSEMINATION = 140

DS_STATE_ALL_STATES = (
    DS_STATE_INITIALIZED, DS_STATE_PROPOSED_FOR_DIGITAL_PRESERVATION,
    DS_STATE_TECHNICAL_METADATA_GENERATED,
    DS_STATE_TECHNICAL_METADATA_GENERATION_FAILED,
    DS_STATE_INVALID_METADATA, DS_STATE_METADATA_VALIDATION_FAILED,
    DS_STATE_VALIDATED_METADATA_UPDATED, DS_STATE_VALID_METADATA,
    DS_STATE_METADATA_CONFIRMED, DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
    DS_STATE_IN_PACKAGING_SERVICE, DS_STATE_PACKAGING_FAILED,
    DS_STATE_SIP_SENT_TO_INGESTION_IN_DPRES_SERVICE,
    DS_STATE_IN_DIGITAL_PRESERVATION,
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
    DS_STATE_IN_DISSEMINATION
)


class MetaxConnectionError(Exception):
    """Exception raised when Metax is not available"""
    message = 'No connection to Metax'


class DatasetNotFoundError(Exception):
    """Exception raised when dataset is not found from metax"""


class Metax(object):
    """Get metadata from metax as dict object."""

    def __init__(self, config_file):
        """ Initialize Metax object.

        :config_file: Global configuration file
        """
        configuration = Configuration(config_file)
        self.metax_url = configuration.get('metax_url')
        self.username = configuration.get('metax_user')
        self.password = configuration.get('metax_password')
        self.baseurl = self.metax_url + '/rest/v1/'
        self.elasticsearch_url = self.metax_url + '/es/'

    def get_dataset(self, dataset_id):
        """Get dataset metadata from Metax.

        :dataset_id: ID number of object
        :returns: dict
        """
        url = self.baseurl + 'datasets' + '/' + dataset_id

        response = _do_get_request(url, HTTPBasicAuth(self.username,
                                                      self.password))

        if response.status_code == 404:
            raise DatasetNotFoundError(
                "Could not find metadata for dataset: %s" % dataset_id
            )
        response.raise_for_status()
        return response.json()

    def get_file(self, file_id):
        """Get file metadata from Metax.

        :file_id: ID number of object
        :returns: dict
        """
        url = self.baseurl + 'files' + '/' + file_id

        response = _do_get_request(url, HTTPBasicAuth(self.username,
                                                      self.password))

        if response.status_code == 404:
            raise Exception("Could not find metadata for file: %s" % file_id)
        response.raise_for_status()
        return response.json()

    def get_contract(self, contract_id):
        """Get file metadata from Metax.

        :contract_id: ID number of object
        :returns: dict
        """
        url = self.baseurl + 'contracts' + '/' + contract_id

        response = _do_get_request(url, HTTPBasicAuth(self.username,
                                                      self.password))

        if response.status_code == 404:
            raise Exception(
                "Could not find metadata for contract: %s" % contract_id
            )
        response.raise_for_status()
        return response.json()

    def get_xml(self, entity_url, entity_id):
        """Get xml data of dataset, contract or file with id from Metax.

        :entity_url: "datasets", "contracts" or "files"
        :entity_id: ID number of object
        :returns: dict with XML namespace strings as keys and
                  lxml.etree.ElementTree objects as values
        """
        # Init result dict
        xml_dict = {}

        # Get list of xml namespaces
        ns_key_url = self.baseurl + entity_url + '/' + entity_id + '/xml'
        response = _do_get_request(ns_key_url, HTTPBasicAuth(self.username,
                                                             self.password))
        if response.status_code == 404:
            raise Exception("Could not retrieve list of additional metadata "
                            "XML for dataset %s: %s" % (entity_id, ns_key_url))
        response.raise_for_status()
        ns_key_list = response.json()

        # For each listed namespace, download the xml, create ElementTree, and
        # add it to result dict
        for ns_key in ns_key_list:
            query = '?namespace=' + ns_key
            response = _do_get_request(ns_key_url + query,
                                       HTTPBasicAuth(self.username,
                                                     self.password))
            if not response.status_code == 200:
                raise Exception("Could not retrieve additional metadata XML "
                                "for dataset %s: %s" % (entity_id,
                                                        ns_key_url + query))
            # pylint: disable=no-member
            xml_dict[ns_key] = lxml.etree.fromstring(response.content)\
                .getroottree()

        return xml_dict

    def set_xml(self, file_id, xml_element):
        """Add xml data to a file with in Metax.

        :file_id: ID of file
        :xml_element: XML Element
        :returns: None
        """
        # Read namespace from XML element
        namespace = \
            xml_element.attrib[
                '{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'
            ].split()[0]

        # Convert XML element to string
        data = lxml.etree.tostring(xml_element, pretty_print=True)

        # POST to Metax
        url = '%sfiles/%s/xml?namespace=%s' % (self.baseurl,
                                               file_id,
                                               namespace)
        headers = {'Content-Type': 'application/xml'}
        response = requests.post(url,
                                 data=data,
                                 headers=headers,
                                 auth=HTTPBasicAuth(self.username,
                                                    self.password))

        response.raise_for_status()
        if response.status_code != 201:
            raise requests.exceptions.HTTPError(
                "Expected 201 Created, got {} instead".format(
                    response.status_code)
            )

    def get_elasticsearchdata(self):
        """Get elastic search data from Metax

        :returns: dict"""
        url = self.elasticsearch_url + "reference_data/use_category/_search?"\
                                       "pretty&size=100"
        response = _do_get_request(url)

        if response.status_code == 404:
            raise Exception("Could not find elastic search data.")
        response.raise_for_status()
        return response.json()

    def set_preservation_state(self, dataset_id, state, description):
        """Set values of attributes `preservation_state` and
        `preservation_state_description` for dataset in Metax

        0 = Initialized
        10 = Proposed for digital preservation
        20 = Technical metadata generated
        30 = Technical metadata generation failed
        40 = Invalid metadata
        50 = Metadata validation failed
        60 = Validated metadata updated
        70 = Valid metadata
        80 = Accepted to digital preservation
        90 = in packaging service
        100 = Packaging failed
        110 = SIP sent to ingestion in digital preservation service
        120 = in digital preservation
        130 = Rejected in digital preservation service
        140 = in dissemination

        :dataset_id: The ID of dataset in Metax
        :state (integer): The value for `preservation_state`
        :description (string): The value for `preservation_system_description`
        :returns: None

        """
        url = self.baseurl + 'datasets/' + dataset_id
        data = {"preservation_state": state,
                "preservation_description": description}
        response = requests.patch(
            url,
            json=data,
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if response.status_code == 503:
            raise MetaxConnectionError

        # Raise exception if request fails
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise Exception(response.json())

    def set_file_characteristics(self, file_id, file_characteristics):
        """Updates `file_characteristics` attribute for a file in Metax.

        :file_id: ID of file
        :file_characteristics: Dict object that contains new data for
                               `file_characteristics` attribute
        :returns: None
        """

        url = self.baseurl + 'files/' + file_id
        data = {"file_characteristics": file_characteristics}

        response = requests.patch(
            url,
            json=data,
            auth=HTTPBasicAuth(self.username, self.password)
        )

        if response.status_code == 503:
            raise MetaxConnectionError

        # Raise exception if request fails
        response.raise_for_status()

    def get_datacite(self, dataset_id):
        """Get descriptive metadata in datacite xml format.

        :dataset_id: ID of dataset
        :returns: Datacite XML (lxml.etree.ElementTree object)
        """
        url = "%sdatasets/%s?dataset_format=datacite" % (self.baseurl,
                                                         dataset_id)
        response = _do_get_request(url, HTTPBasicAuth(self.username,
                                                      self.password))

        if response.status_code == 404:
            raise Exception("Could not find descriptive metadata.")
        response.raise_for_status()

        # pylint: disable=no-member
        return lxml.etree.fromstring(response.content).getroottree()

    def get_dataset_files(self, dataset_id):
        """Get file metadata of dataset from Metax.

        :dataset_id: ID number of object
        :returns: dict"""
        url = self.baseurl + 'datasets/' + dataset_id + '/files'

        response = _do_get_request(url, HTTPBasicAuth(self.username,
                                                      self.password))

        if response.status_code == 404:
            raise Exception("Could not find dataset files metadata.")
        response.raise_for_status()

        return response.json()


def _do_get_request(url, auth=None):
    """Wrapper function for requests.get() function. Raises
    MetaxConnectionError if status code is 503 (Service unavailable

    :returns: requests response
    """
    response = requests.get(url, auth=auth)
    if response.status_code == 503:
        raise MetaxConnectionError
    return response
