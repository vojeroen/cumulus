import copy
import random
import uuid

from mongoengine import EmbeddedDocument, StringField, IntField, Document, ReferenceField, EmbeddedDocumentField, \
    EmbeddedDocumentListField
from nimbus.helpers.timestamp import get_utc_int
from pyeclib.ec_iface import ECDriver, ECInsufficientFragments

from app.models.error import HashError, ReconstructionError, NoRemoteStorageLocationFound, RemoteStorageError
from app.models.fragment import Fragment, OrphanedFragment
from app.models.hub import Hub
from app.models.local import LocalFile


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


class Collection(EmbeddedDocument):
    name = StringField(required=True)


class Encoding(EmbeddedDocument):
    name = StringField(required=True)
    k = IntField(required=True)  # number of file pieces
    m = IntField(required=True)  # number of parity blocks


class File(Document):
    uuid = StringField(primary_key=True, default=lambda: uuid.uuid4().hex)
    timestamp_created = IntField(required=True, default=get_utc_int)
    source = ReferenceField('Hub', required=True)
    collection = EmbeddedDocumentField(Collection, required=True)
    filename = StringField(required=True)
    hash = StringField(required=True)
    encoding = EmbeddedDocumentField(Encoding, required=True)
    fragments = EmbeddedDocumentListField(Fragment, required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_file = None

    def __str__(self):
        return self.__class__.__name__ + ':' + self.source.cumulus_id + \
               ':' + self.collection.name + ':' + self.filename

    def __enter__(self):
        if self._local_file is not None:
            raise RuntimeError('Cannot use the same File as context manager in its own context.')
        if self.source is None:
            raise ValueError('You must define the source before using the File.')
        if self.collection is None:
            raise ValueError('You must define the collection before using the File.')
        if self.filename is None:
            raise ValueError('You must define the filename before using the File.')
        if self.encoding is None:
            raise ValueError('You must define the encoding before using the File.')
        self._download_from_storage()
        return self._local_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._local_file.hash != self.hash:
                orphan_fragments = self._remove_fragments(delay=True)
                self._upload_to_storage()
            else:
                orphan_fragments = []
            self._local_file.remove()
            self._local_file = None
            self.save()
            for orphan_fragment in orphan_fragments:
                orphan_fragment.save()
        except (RemoteStorageError, NoRemoteStorageLocationFound):
            # if upload fails: remove previously uploaded fragments, clean up and raise
            self._remove_fragments()
            self._local_file.remove()
            self._local_file = None
            raise

    def _ecdriver(self):
        return ECDriver(
            k=self.encoding.k,
            m=self.encoding.m,
            ec_type=self.encoding.name
        )

    def _download_from_storage(self):
        """
        Download the file from the storage. Only the minimum amount of fragments is downloaded to do this.
        """
        if self.fragments.count() == 0:
            self._local_file = LocalFile()
            return

        ecd = self._ecdriver()
        fragment_data = []
        for fragment in self.fragments:
            try:
                with fragment as fr:
                    fragment_data.append(fr.read())
            except HashError:
                # TODO mark fragment as dirty to be replaced
                pass
            if len(fragment_data) >= self.encoding.k:
                break
        try:
            self._local_file = LocalFile(content=ecd.decode(fragment_data), content_hash=self.hash)
        except ECInsufficientFragments:
            raise ReconstructionError('There are not enough fragments to reconstruct {}'.format(self))

    def _upload_to_storage(self):
        """
        Split a local file into fragments, assign hubs to these fragments and upload the fragments.
        :param local_file: 
        :return: 
        """
        self.hash = self._local_file.hash
        ecd = self._ecdriver()
        exclude_hubs_for_storage = []
        for fragment_index, fragment_data in enumerate(ecd.encode(self._local_file.read())):
            while True:
                remote = select_remote_storage_location(
                    file=self,
                    size=int((self._local_file.size / self.encoding.k) * 1.10),
                    exclude_locations=exclude_hubs_for_storage
                )
                fragment = Fragment(index=fragment_index, remote=remote)
                try:
                    with fragment as fr:
                        fr.write(fragment_data)
                except RemoteStorageError:
                    exclude_hubs_for_storage.append(remote)
                    continue
                self.fragments.append(fragment)
                break

    def _remove_fragments(self, delay=False):
        """
        Remove all fragments from the storage by converting them to orphaned fragments.
        :return: 
        """
        orphan_fragments = []
        for fragment in copy.copy(self.fragments):
            orphan_fragment = OrphanedFragment.create_from(fragment)
            orphan_fragment.file = self
            orphan_fragments.append(orphan_fragment)
            self.fragments.remove(fragment)

        if not delay:
            for orphan_fragment in orphan_fragments:
                orphan_fragment.save()

        return orphan_fragments

    def verify_full(self):
        """
        Verify all fragments fully.
        Returns True of verification succeeded, False if not.
        :return: bool 
        """
        return all([fragment.verify_full() for fragment in self.fragments])

    def verify_hash(self):
        """
        Verify the hash of all fragments.
        Returns True of verification succeeded, False if not.
        :return: bool
        """
        return all([fragment.verify_hash() for fragment in self.fragments])

    def remove(self):
        self._remove_fragments()
        self.delete()
