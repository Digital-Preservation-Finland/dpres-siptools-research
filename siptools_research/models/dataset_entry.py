"""Model for file errors"""
import datetime

from mongoengine import (BooleanField, DateTimeField, Document,
                         EmbeddedDocument, EmbeddedDocumentField,
                         MapField, StringField)


class TaskEntry(EmbeddedDocument):
    timestamp = DateTimeField(
        default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    # Name implies this should be a list; string type is maintained for
    # backwards compatibility
    messages = StringField()

    result = StringField()


class DatasetWorkflowEntry(Document):
    id = StringField(db_field="_id", primary_key=True)
    """Dataset identifier"""

    enabled = BooleanField(default=False)

    target = StringField()

    workflow_tasks = MapField(EmbeddedDocumentField(TaskEntry))
    """'Task name' -> 'task entry' mapping"""

    meta = {
        "collection": "workflow",  # For backwards compatibility
        "db_alias": "siptools_research",

        # Stop MongoEngine from complaining about extra fields 'status',
        # 'disabled', 'dataset' and 'completed' that are no longer used by
        # siptools-research, but might exist in older documents.
        # TODO: Any outdated documents should be cleaned from databases.
        "strict": False,
    }
