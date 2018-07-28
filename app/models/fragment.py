import uuid

import requests
from mongoengine import EmbeddedDocument, StringField, IntField, ReferenceField, Document
from nimbus import config
from nimbus.client import Client
from nimbus.helpers.timestamp import get_utc_int

from app.models.error import UploadFailed, DownloadFailed, DeleteFailed
from app.models.local import LocalFragment

CONNECT_URL = 'tcp://{}:{}'.format(config.get('requests', 'client_hostname'),
                                   config.get('requests', 'client_port'))

CLIENT = Client(connect=CONNECT_URL)


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
            raise ValueError('You must define the remote before using the Fragment.')
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
        response = CLIENT.post('file', data={
            'uuid': self.uuid,
            'content': self._local_fragment.read()
        })
        if response.status_code != requests.codes.ok:
            raise UploadFailed()

    def _download_from_storage(self):
        """
        Download a fragment from the storage.
        :return: LocalFragment
        """
        response = CLIENT.get('file', parameters={'uuid': self.uuid}, decode_response=False)
        if response.status_code not in (requests.codes.ok, requests.codes.not_found):
            raise DownloadFailed()
        self._local_fragment = LocalFragment(content=response.response[b'content'], content_hash=self.hash)

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
        response = CLIENT.delete('file', parameters={'uuid': self.uuid})
        if response.status_code != requests.codes.ok:
            raise DeleteFailed()
        self.delete()
