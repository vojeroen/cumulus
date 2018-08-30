import copy
import random
import uuid

from mongoengine import EmbeddedDocument, StringField, IntField, Document, ReferenceField, EmbeddedDocumentField, \
    EmbeddedDocumentListField
from nimbus.errors import ConnectionTimeoutError
from nimbus.helpers.timestamp import get_utc_int
from pyeclib.ec_iface import ECDriverError

from app.models.cache.file import CachedFile, ecdriver
from app.models.error import ReconstructionError, NoRemoteStorageLocationFound, RemoteStorageError, HashError
from app.models.fragment import Fragment, OrphanedFragment
from app.models.hub import Hub


def select_remote_storage_location(file, size, exclude_locations=None):
    # naive approach:
    # - never include the source
    # - never include explicitly excluded locations
    # - when there are no valid storage locations anymore, just use already used locations

    base_exclude = {file.source}.union(set(exclude_locations))

    exclude = copy.copy(base_exclude)
    for fragment in file.fragments:
        exclude.add(fragment.remote)

    while True:
        query = Hub.objects \
            .filter(cumulus_id__nin=[h.cumulus_id for h in exclude]) \
            .filter(available_bytes__gt=size)
        hub_count = query.count()
        if hub_count == 0:
            if exclude == base_exclude:
                raise NoRemoteStorageLocationFound
            else:
                exclude = copy.copy(base_exclude)
        else:
            break

    hub_select = random.randint(0, hub_count - 1)
    return query[hub_select]


class Encoding(EmbeddedDocument):
    name = StringField(required=True)
    k = IntField(required=True)  # number of file pieces
    m = IntField(required=True)  # number of parity blocks


class File(Document):
    uuid = StringField(primary_key=True, default=lambda: uuid.uuid4().hex)
    timestamp_created = IntField(required=True, default=get_utc_int)
    source = ReferenceField('Hub', required=True)
    collection = StringField(required=True)
    filename = StringField(required=True)
    hash = StringField(required=True)
    encoding = EmbeddedDocumentField(Encoding, required=True)
    fragments = EmbeddedDocumentListField(Fragment, required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = None

    def __str__(self):
        return self.__class__.__name__ + ':' + self.source.cumulus_id + \
               ':' + self.collection + ':' + self.filename

    def __enter__(self):
        if self._cache is not None:
            raise RuntimeError('Cannot use the same File as context manager in its own context.')
        if self.source is None:
            raise ValueError('You must define the source before using the File.')
        if self.collection is None:
            raise ValueError('You must define the collection before using the File.')
        if self.filename is None:
            raise ValueError('You must define the filename before using the File.')
        if self.encoding is None:
            raise ValueError('You must define the encoding before using the File.')
        self._cache = CachedFile(encoding=self.encoding, fragments=self.fragments, expected_hash=self.hash)
        return self._cache

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.hash != self._cache.hash:
                orphan_fragments = self._remove_fragments(delay=True)
                self._upload_content()
            else:
                orphan_fragments = []
            self._cache.close()
            self._cache = None
            self.save()
            for orphan_fragment in orphan_fragments:
                orphan_fragment.save()
        except (RemoteStorageError, NoRemoteStorageLocationFound):
            # if upload fails: remove previously uploaded fragments, clean up and raise
            self._remove_fragments()
            self._cache.close()
            self._cache = None
            raise

    def _upload_content(self):
        self.hash = self._cache.hash
        ecd = ecdriver(self.encoding)
        exclude_hubs_for_storage = []
        for fragment_index, fragment_data in enumerate(ecd.encode(self._cache.read())):
            while True:
                remote = select_remote_storage_location(
                    file=self,
                    size=int((self._cache.size / self.encoding.k) * 1.10),
                    exclude_locations=exclude_hubs_for_storage
                )
                fragment = Fragment(index=fragment_index, remote=remote)
                try:
                    with fragment as fr:
                        fr.write(fragment_data)
                except (RemoteStorageError, ConnectionTimeoutError):
                    exclude_hubs_for_storage.append(remote)
                    continue
                self.fragments.append(fragment)
                break

    def _remove_fragments(self, delay=False):
        orphan_fragments = []
        for fragment in copy.copy(self.fragments):
            orphan_fragment = OrphanedFragment.create_from(fragment)
            orphan_fragment.file = self.uuid
            orphan_fragments.append(orphan_fragment)
            self.fragments.remove(fragment)

        if not delay:
            for orphan_fragment in orphan_fragments:
                orphan_fragment.save()

        return orphan_fragments

    def reconstruct(self):
        if self._cache is not None:
            raise RuntimeError('Cannot call this function when in a context manager.')

        ecd = ecdriver(self.encoding)

        # retrieve data to be used for reconstruction
        fragment_data = []
        while True:
            reconstruction_indexes = [f.index for f in self.fragments.filter(is_clean=False)]
            try:
                indexes = ecd.fragments_needed(reconstruction_indexes)
            except ECDriverError:
                raise ReconstructionError('There are not enough fragments to reconstruct {}'.format(self))
            for index in indexes:
                fragment = self.fragments.filter(index=index).first()
                try:
                    with fragment as fr:
                        fragment_data.append(fr.read())
                except (RemoteStorageError, HashError):
                    fragment.is_clean = False
                    fragment.save()
            if len(fragment_data) >= len(indexes):
                break

        # reconstruct
        reconstruction_data = ecd.reconstruct(fragment_data, reconstruction_indexes)
        for index, data in zip(reconstruction_indexes, reconstruction_data):
            fragment = self.fragments.filter(index=index).first()
            # TODO select another remote for the fragment if necessary
            fragment.is_clean = True
            fragment.save()
            with fragment as fr:
                fr.write(data)

    def verify_full(self):
        if self._cache is not None:
            raise RuntimeError('Cannot call this function when in a context manager.')
        is_clean = all([fragment.verify_full() for fragment in self.fragments])
        self.save()
        return is_clean

    def verify_hash(self):
        if self._cache is not None:
            raise RuntimeError('Cannot call this function when in a context manager.')
        is_clean = all([fragment.verify_hash() for fragment in self.fragments])
        self.save()
        return is_clean

    def remove(self):
        if self._cache is not None:
            raise RuntimeError('Cannot call this function when in a context manager.')
        self._remove_fragments()
        self.delete()
