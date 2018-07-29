import os
import uuid

from Crypto.Hash import SHA3_256

from app.models.error import HashError

LOCAL_CACHE = 'cache'


class LocalFile:
    def __init__(self, content=None, content_hash=None, file_path=None):
        """
        :param content: bytes or generator containing the file content
        :param file_path: absolute or relative file path 
        """
        if file_path is not None:
            self._file_path = file_path
        else:
            self._file_path = os.path.join(LOCAL_CACHE, uuid.uuid4().hex)

        if content is not None:
            self.write(content)
            if content_hash and self.hash != content_hash:
                self.remove()
                raise HashError('The {} does not match the hash: '.format(self.__class__.__name__) +
                                '{}'.format(self._file_path))

    @property
    def hash(self):
        chunk_size = 1024 * 1024  # 1 MB
        hasher = SHA3_256.new()
        for chunk in self.read_chunks(chunk_size):
            hasher.update(chunk)
        return hasher.hexdigest()

    @property
    def size(self):
        return os.path.getsize(self._file_path)

    def _open(self, mode):
        open(self._file_path, 'ab').close()
        return open(self._file_path, mode)

    def read(self):
        with self._open('rb') as f:
            return f.read()

    def read_chunks(self, chunk_size=1024 * 1024):
        with self._open('rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    break

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
        with self._open('ab') as f:
            self._write(f, content)

    def remove(self):
        try:
            os.remove(self._file_path)
        except OSError:
            pass


class LocalFragment(LocalFile):
    pass
