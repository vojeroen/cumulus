import uuid

from mongoengine import Document, StringField


class Hub(Document):
    reference = StringField(required=True)
    cumulus_id = StringField(primary_key=True, default=lambda: 'CML-' + uuid.uuid4().hex)
