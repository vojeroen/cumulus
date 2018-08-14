import requests

from app.models.cache import CachedObject, get_client
from app.models.error import DownloadFailed, InsufficientStorageSpace, UploadFailed, DeleteFailed


def download_fragment_hash(hub, fragment_uuid):
    response = get_client().get(hub.cumulus_id + '/hash', parameters={'uuid': fragment_uuid})
    if response.status_code not in (requests.codes.ok, requests.codes.not_found):
        raise DownloadFailed()
    return response.response['hash']


def download_fragment_content(hub, fragment_uuid):
    response = get_client().get(hub.cumulus_id + '/file', parameters={'uuid': fragment_uuid}, decode_response=False)
    if response.status_code not in (requests.codes.ok, requests.codes.not_found):
        raise DownloadFailed()
    return response.response[b'content']


def upload_fragment_content(hub, fragment_uuid, content):
    response = get_client().post(hub.cumulus_id + '/file', data={
        'uuid': fragment_uuid,
        'content': content
    })

    if response.status_code == requests.codes.ok:
        store_available_bytes(hub, response.response['available_bytes'])
    elif response.status_code == requests.codes.forbidden:
        store_available_bytes(hub, response.response['available_bytes'])
        raise InsufficientStorageSpace()
    else:
        raise UploadFailed()


def remove_fragment_content(hub, fragment_uuid):
    response = get_client().delete(hub.cumulus_id + '/file', parameters={'uuid': fragment_uuid})

    if response.status_code == requests.codes.ok:
        store_available_bytes(hub, response.response['available_bytes'])
    else:
        raise DeleteFailed()


def store_available_bytes(hub, available_bytes):
    hub.available_bytes = available_bytes
    hub.save()


class CachedFragment(CachedObject):
    def __init__(self, remote, uuid, *args, **kwargs):
        self._remote = remote
        self._uuid = uuid
        super().__init__(*args, **kwargs)

    def download_content(self):
        content = download_fragment_content(self._remote, self._uuid)
        self.write(content)

    def upload_content(self):
        upload_fragment_content(self._remote, self._uuid, self.read())
