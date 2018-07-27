import copy
import uuid

from mongoengine import EmbeddedDocument, StringField, IntField, Document, ReferenceField, EmbeddedDocumentField, \
    EmbeddedDocumentListField
from nimbus.helpers.timestamp import get_utc_int
from pyeclib.ec_iface import ECDriver, ECInsufficientFragments

from app.models.error import HashError, ReconstructionError
from app.models.fragment import Fragment, OrphanedFragment
from app.models.local import LocalFile


class Collection(EmbeddedDocument):
    name = StringField(required=True)


class Encoding(EmbeddedDocument):
    name = StringField(required=True)
    k = IntField(required=True)
    m = IntField(required=True)


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
        if self._local_file.hash != self.hash:
            self._remove_fragments()
            self._upload_to_storage()
            # TODO if upload fails, orphan fragments have to be reset
        self._local_file.remove()
        self._local_file = None
        self.save()

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
        for fragment_index, fragment_data in enumerate(ecd.encode(self._local_file.read())):
            # TODO intelligently choose the remote hub
            fragment = Fragment(index=fragment_index, remote=self.source)
            with fragment as fr:
                fr.write(fragment_data)
            self.fragments.append(fragment)

    def _remove_fragments(self):
        """
        Remove all fragments from the storage by converting them to orphaned fragments.
        :return: 
        """
        for fragment in copy.copy(self.fragments):
            orphan_fragment = OrphanedFragment.create_from(fragment)
            orphan_fragment.file = self
            orphan_fragment.save()
            self.fragments.remove(fragment)

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
