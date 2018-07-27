import os
import uuid

from mongoengine import EmbeddedDocument, StringField, IntField, ReferenceField
from nimbus.helpers.timestamp import get_utc_int

from app.models.local import LocalFragment


class Fragment(EmbeddedDocument):
    uuid = StringField(primary_key=True, default=lambda: uuid.uuid4().hex)
    timestamp_created = IntField(required=True, default=get_utc_int)
    index = IntField(required=True)
    hash = StringField(required=True)
    remote = ReferenceField('Hub', required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_fragment = None

    def __enter__(self):
        if self._local_fragment is not None:
            raise RuntimeError('Cannot use the same Fragment as context manager in its own context.')
        if self.index is None:
            raise ValueError('You must define the index before using the Fragment.')
        if self.remote is None:
            raise ValueError('You must define the remote before using the Framgent.')
        self._download_from_storage()
        return self._local_fragment

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._local_fragment.hash != self.hash:
            self._upload_to_storage()
        self._local_fragment.remove()
        self._local_fragment = None

    def _upload_to_storage(self):
        """
        Upload a local fragment to the storage.
        :param local_fragment: 
        :return: 
        """
        self.hash = self._local_fragment.hash

        # TODO: actual upload
        with open(os.path.join('cache/fragments', self.uuid), 'wb') as f:
            f.write(self._local_fragment.read())

    def _download_from_storage(self):
        """
        Download a fragment from the storage.
        :return: LocalFragment
        """
        # TODO: actual upload
        open(os.path.join('cache/fragments', self.uuid), 'ab').close()
        with open(os.path.join('cache/fragments', self.uuid), 'rb') as f:
            self._local_fragment = LocalFragment(content=f.read(), content_hash=self.hash)

    def verify_full(self):
        """
        Download the fragment from the storage and verify it with the hash. 
        Returns True of verification succeeded, False if not.
        :return: bool
        """
        # TODO: verdere uitwerking
        pass

    def verify_hash(self):
        """
        Request the hash from the storage and verify it.
        Returns True of verification succeeded, False if not.
        :return: bool
        """
        # TODO: verdere uitwerking
        pass
