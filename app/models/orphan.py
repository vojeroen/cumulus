import os

from mongoengine import Document, StringField, ReferenceField, IntField
from nimbus.helpers.timestamp import get_utc_int


class OrphanedFragment(Document):
    uuid = StringField(primary_key=True, required=True)
    timestamp_created = IntField(required=True)
    timestamp_orphaned = IntField(required=True, default=get_utc_int)
    file = ReferenceField('File', required=True)
    index = IntField(required=True)
    hash = StringField(required=True)
    remote = ReferenceField('Hub', required=True)

    @classmethod
    def create_from(cls, fragment):
        orphaned_fragment = cls(
            uuid=fragment.uuid,
            timestamp_created=fragment.timestamp_created,
            index=fragment.index,
            hash=fragment.hash,
            remote=fragment.remote
        )
        return orphaned_fragment

    def remove(self):
        """
        Remove the fragment from the remote storage.
        :return: 
        """
        try:
            os.remove(os.path.join('cache/fragments', self.uuid))
        except OSError:
            pass
        self.delete()
