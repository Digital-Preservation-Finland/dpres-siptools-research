"""Model for file errors"""
import datetime
from enum import Enum

from mongoengine import (DateTimeField, Document, EnumField, ListField,
                         StringField)


class StorageService(Enum):
    PAS = "pas"
    """Either FDDPS pre-ingest storage or DPRES service"""

    IDA = "ida"


class FileError(Document):
    """
    The file errors
    """
    file_id = StringField(required=True)
    """File identifier, corresponding to the `id` file field in Metax"""

    storage_identifier = StringField(
        required=True,
    )
    """Storage identifier of the file"""

    storage_service = EnumField(StorageService, required=True)
    """Storage service of the file"""

    dataset_id = StringField(default=None)
    """Dataset related to this file error, if any.

    This is relevant when the error is related to a file and dataset pairing,
    rather than just the file itself.
    """

    errors = ListField(field=StringField())
    """List of errors"""

    created_at = DateTimeField(
        default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    meta = {
        "indexes": [
            {
                "fields": (
                    # These fields uniquely identify an error
                    "storage_identifier", "storage_service", "dataset_id"
                ),
                "unique": True
            },
            {
                "fields": ("file_id",),
                "unique": True
            }
        ],
        "db_alias": "siptools_research"
    }
