import os
import uuid

from Crypto.Hash import SHA3_256
from nimbus import config
from nimbus.client import Client

from app.models.error import RemoteStorageError, HashError

LOCAL_CACHE = 'cache'
CONNECT_URL = 'tcp://{}:{}'.format(config.get('requests', 'client_hostname'),
                                   config.get('requests', 'client_port'))
CLIENT = Client(connect=CONNECT_URL)


class CachedObject:
    def __init__(self, expected_hash=None, file_path=None):
        self._initial_hash = None
        self._expected_hash = expected_hash

        if file_path is not None:
            self._file_path = file_path
        else:
            self._file_path = os.path.join(LOCAL_CACHE, uuid.uuid4().hex)

    # PROPERTIES
    @property
    def hash(self):
        self._download_content()
        chunk_size = 1024 * 1024  # 1 MB
        hasher = SHA3_256.new()
        with self._open('rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    hasher.update(chunk)
                else:
                    break
        return hasher.hexdigest()

    @property
    def size(self):
        self._download_content_and_check_hash()
        return os.path.getsize(self._file_path)

    # HELPERS
    def _open(self, mode):
        return open(self._file_path, mode)

    # READ
    def download_content(self):
        raise NotImplementedError

    def _download_content(self):
        if not os.path.isfile(self._file_path):
            self.download_content()
            self._initial_hash = self.hash
            return True
        else:
            return False

    def _check_hash(self):
        if self._expected_hash and self._expected_hash != self.hash:
            self.cleanup()
            raise HashError(
                'Hash of {} {} is different from the expected hash ({} - {})'.format(
                    self.__class__.__name__,
                    self._file_path,
                    self.hash,
                    self._expected_hash
                )
            )

    def _download_content_and_check_hash(self):
        if self._download_content():
            self._check_hash()

    def read(self):
        self._download_content_and_check_hash()
        with self._open('rb') as f:
            return f.read()

    def read_chunks(self, chunk_size=1024 * 1024):
        self._download_content_and_check_hash()
        with self._open('rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    break

    # WRITE
    @staticmethod
    def _write(file_object, content):
        if isinstance(content, (bytes, str)):
            # we can't write str, but we'll let the write function handle this
            file_object.write(content)
        else:
            for c in content:
                file_object.write(c)

    def write(self, content):
        with self._open('wb') as f:
            self._write(f, content)

    def append(self, content):
        self._download_content_and_check_hash()
        with self._open('ab') as f:
            self._write(f, content)

    # CLOSE
    def upload_content(self):
        raise NotImplementedError

    def _upload_content(self):
        self.upload_content()

    def cleanup(self):
        try:
            os.remove(self._file_path)
        except OSError:
            pass

    def close(self):
        try:
            if self.hash != self._initial_hash:
                self._upload_content()
        except RemoteStorageError:
            self.cleanup()
            raise
        else:
            self.cleanup()
