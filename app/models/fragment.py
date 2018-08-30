import uuid

from mongoengine import EmbeddedDocument, StringField, IntField, ReferenceField, Document, BooleanField

from app.models.cache.fragment import CachedFragment, remove_fragment_content, download_fragment_hash
from app.models.error import RemoteStorageError, \
    HashError
from nimbus.errors import ConnectionTimeoutError
from nimbus.helpers.timestamp import get_utc_int


class Fragment(EmbeddedDocument):
    uuid = StringField(primary_key=True, default=lambda: uuid.uuid4().hex)
    timestamp_created = IntField(required=True, default=get_utc_int)
    index = IntField(required=True)
    remote = ReferenceField('Hub', required=True)
    hash = StringField(required=True)
    is_clean = BooleanField(required=True, default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = None

    def __enter__(self):
        if self._cache is not None:
            raise RuntimeError('Cannot use the same Fragment as context manager in its own context.')
        if self.index is None:
            raise ValueError('You must define the index before using the Fragment.')
        if self.remote is None:
            raise ValueError('You must define the remote before using the Fragment.')
        self._cache = CachedFragment(remote=self.remote, uuid=self.uuid, expected_hash=self.hash)
        return self._cache

    def __exit__(self, exc_type, exc_val, exc_tb):
        new_hash = self._cache.hash
        try:
            self._cache.close()
        except (RemoteStorageError, ConnectionTimeoutError):
            # if upload fails: clean up and raise
            self._cache = None
            raise
        else:
            self.hash = new_hash
            self._cache = None

    def verify_full(self):
        if self._cache is not None:
            raise RuntimeError('Cannot verify a Fragment when in a context manager.')
        try:
            with self as fr:
                fr.read()
        except ConnectionTimeoutError:
            self.is_clean = False
        except HashError:
            self.is_clean = False
        else:
            self.is_clean = True
        return self.is_clean

    def verify_hash(self):
        try:
            fragment_hash = download_fragment_hash(self.remote, self.uuid)
        except ConnectionTimeoutError:
            self.is_clean = False
        else:
            self.is_clean = fragment_hash == self.hash
        return self.is_clean


class OrphanedFragment(Document):
    uuid = StringField(primary_key=True, required=True)
    timestamp_created = IntField(required=True)
    timestamp_orphaned = IntField(required=True, default=get_utc_int)
    file = StringField(required=True)
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
        remove_fragment_content(self.remote, self.uuid)
        self.delete()
