"""Model for file errors."""
import datetime

from mongoengine import (
    DateTimeField,
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    MapField,
    StringField,
)


class TaskEntry(EmbeddedDocument):
    timestamp = DateTimeField(
        default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    # Name implies this should be a list; string type is maintained for
    # backwards compatibility
    messages = StringField()

    result = StringField()


class DatasetEntry(Document):
    id = StringField(db_field="_id", primary_key=True)
    """Dataset identifier"""

    workflow_tasks = MapField(EmbeddedDocumentField(TaskEntry))
    """'Task name' -> 'task entry' mapping"""

    meta = {
        "collection": "dataset",
        "db_alias": "siptools_research",
    }
