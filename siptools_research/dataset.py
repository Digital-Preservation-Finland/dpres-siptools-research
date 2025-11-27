"""Dataset class."""

import datetime

from siptools_research.metax import get_metax_client
from siptools_research.models.dataset_entry import (
    DatasetEntry,
    TaskEntry,
)
from siptools_research.models.file_error import FileError

PAS_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-pas"
IDA_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-ida"


def _timestamp():
    """Return time now.

    :returns: ISO 8601 string
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class Dataset:
    """Class that represents a dataset.

    This class manages dataset entry in Metax, and takes care of
    juggling with different versions the dataset (the original version
    and the PAS data catalog version). Also stores information about
    errors found in the dataset.
    """

    def __init__(
        self, identifier, document=None, config="/etc/siptools_research.conf"
    ):
        """Initialize dataset."""
        self.identifier = identifier
        self._cached_metadata = None
        self._metax_client = get_metax_client(config)

        self._document = document
        if self._document is None:
            try:
                self._document = \
                    DatasetEntry.objects.get(id=self.identifier)
            except DatasetEntry.DoesNotExist:
                self._document = DatasetEntry(
                    id=self.identifier
                )

    @property
    def _metadata(self):
        """Dataset metadata from Metax."""
        if self._cached_metadata is None:
            self._cached_metadata = self._metax_client.get_dataset(
                self.identifier
            )

        return self._cached_metadata

    @property
    def has_been_copied_to_pas_datacatalog(self):
        return self._metadata["preservation"]["dataset_version"]["id"]\
            is not None

    @property
    def sip_identifier(self):
        """The SIP identifier of the dataset.

        The SIP identifier is the DOI of the dataset version which is in
        PAS data catalog.
        """
        if self._metadata["data_catalog"] == PAS_DATA_CATALOG_IDENTIFIER:
            # Dataset was originally created to PAS data catalog.
            identifier = self._metadata["persistent_identifier"]

        elif self._metadata["data_catalog"] == IDA_DATA_CATALOG_IDENTIFIER:

            if self.has_been_copied_to_pas_datacatalog:
                # Dataset was created in IDA catalog, and it has been
                # copied to PAS data catalog. The preferred identifier
                # of the PAS version of the dataset will be used as SIP
                # identifier.
                identifier = (self._metadata["preservation"]["dataset_version"]
                              ["persistent_identifier"])
            else:
                # Dataset has not yet been copied to PAS data catalog.
                error = ("The dataset has not been copied to PAS data"
                         "catalog, so DOI does not exist.")
                raise ValueError(error)
        else:
            error = f"Unknown data catalog: {self._metadata['data_catalog']}"
            raise ValueError(error)

        return identifier

    @property
    def preservation_state(self):
        """Preservation state of the dataset."""
        if self.has_been_copied_to_pas_datacatalog:
            state = self._metadata["preservation"]["dataset_version"][
                "preservation_state"
            ]
        else:
            state = self._metadata["preservation"]["state"]

        return state

    def set_preservation_reason(self, reason: str):
        """Set preservation reason description of the dataset."""
        self._metax_client.set_preservation_reason(self.pas_dataset_id, reason)

    def set_preservation_state(self, state, description):
        """Set preservation state of the dataset.

        If dataset has been copied to PAS data catalog, the preservation
        state of the PAS version is set.
        """
        if self.has_been_copied_to_pas_datacatalog:
            preserved_dataset_id = self._metadata["preservation"][
                "dataset_version"]["id"]
        else:
            preserved_dataset_id = self._metadata["id"]

        self._metax_client.set_preservation_state(
            preserved_dataset_id, state, description
        )

    def mark_preserved(self):
        """Mark dataset preserved.

        Sets `pas_package_created` value to True, so other services know
        that the dataset is in digital preservation.

        If dataset has been copied to PAS data catalog, the value is
        updated in PAS version, not in the original version.
        """
        if self.has_been_copied_to_pas_datacatalog:
            preserved_dataset_id = self._metadata["preservation"][
                "dataset_version"]["id"]
        else:
            preserved_dataset_id = self._metadata["id"]

        self._metax_client.set_pas_package_created(preserved_dataset_id)

    @property
    def is_preserved(self):
        """Check if dataset already is in DPS."""
        return self._metadata["preservation"]["pas_package_created"]

    def lock(self):
        """Lock dataset."""
        self._metax_client.lock_dataset(self.identifier)

    def unlock(self):
        """Unlock dataset."""
        self._metax_client.unlock_dataset(self.identifier)

        # When dataset is unlocked, it can be edited by users.
        # Therefore, the previously found errors might not exist
        # anymore.
        self._document.errors.clear()
        self._document.save()

        # Also the file metadata can be edited, and files could be even
        # removed from dataset. Therefore, also file errors must be
        # deleted.
        dataset_files = self._metax_client.get_dataset_files(self.identifier)
        FileError.objects.filter(
            file_id__in=[file["id"] for file in dataset_files],
            dataset_id__in=(None, self.identifier)
        ).delete()

    def copy_to_pas_datacatalog(self):
        """Copy dataset to PAS data catalog."""
        if self._metadata["data_catalog"] != PAS_DATA_CATALOG_IDENTIFIER \
                and not self.has_been_copied_to_pas_datacatalog:
            self._metax_client.copy_dataset_to_pas_catalog(self.identifier)

    def get_datacite(self) -> bytes:
        """Get Datacite document for dataset's PAS data catalog copy."""
        return self._metax_client.get_datacite(self.pas_dataset_id)

    @property
    def pas_dataset_id(self):
        """Dataset identifier of the dataset using the PAS data catalog.

        If the dataset was created in IDA, this is the same as the copy
        created using :meth:`copy_to_pas_datacatalog`. If not, this is
        the dataset identifier itself.
        """
        return (
            self._metadata["preservation"]["dataset_version"]["id"]
            or self._metadata["id"]
        )

    @property
    def errors(self):
        """List errors found in dataset.

        If dataset has any errors, it is invalid and can not be
        preserved.
        """
        return self._document.errors

    def add_error(self, error):
        """Add error to dataset."""
        self._document.errors.append(error)
        self._document.save()

    # TODO: These task logs are not required anymore for anything else
    # than debugging purposes. Is anyone really using them, or could
    # they be removed? The luigi logs contain more or less the same
    # information.
    def log_task(self, task_name, result, message):
        """Log workflow event.

        :param task_name: Name of the task
        :param result: Result string ('failure' or 'success')
        :param messages: Information about the task
        :returns: ``None``
        """
        self._document.workflow_tasks[task_name] = TaskEntry(
            timestamp=_timestamp(),
            messages=message,
            result=result
        )
        self._document.save()
