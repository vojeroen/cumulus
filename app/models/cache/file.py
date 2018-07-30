from pyeclib.ec_iface import ECDriver, ECInsufficientFragments

from app.models.cache import CachedObject
from app.models.error import HashError, ReconstructionError


def ecdriver(encoding):
    return ECDriver(
        k=encoding.k,
        m=encoding.m,
        ec_type=encoding.name
    )


def download_file_content(encoding, fragments):
    ecd = ecdriver(encoding)
    fragment_data = []
    for fragment in fragments:
        try:
            with fragment as fr:
                fragment_data.append(fr.read())
        except HashError:
            fragment.is_clean = False
            fragment.save()
            # TODO send out signal to reconstruct this file
        if len(fragment_data) >= encoding.k:
            break
    return ecd.decode(fragment_data)


class CachedFile(CachedObject):
    def __init__(self, encoding, fragments, *args, **kwargs):
        self._encoding = encoding
        self._fragments = fragments
        super().__init__(*args, **kwargs)

    def download_content(self):
        if self._fragments.count() == 0:
            return

        try:
            content = download_file_content(self._encoding, self._fragments)
        except ECInsufficientFragments:
            raise ReconstructionError(
                'There are not enough fragments to reconstruct the file {}'.format(self._file_path)
            )
        self.write(content)

    def upload_content(self):
        # this is done on the File itself
        pass
