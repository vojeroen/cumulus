import uuid

from mongoengine import Document, StringField, IntField


class Hub(Document):
    reference = StringField(required=True)
    cumulus_id = StringField(primary_key=True, default=lambda: 'CML-' + uuid.uuid4().hex)
    available_bytes = IntField(required=True, default=1 * 1024 * 1024 * 1024 * 1024)  # set default to 1 TB available
