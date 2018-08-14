import os
import shutil

import requests
from Crypto.Hash import SHA3_256
from nimbus import config
from nimbus.worker.context import ctx_request
from nimbus.worker.worker import Worker

STORAGE_DIR = 'cache/storage4'
MINIMUM_FREE_MB = 128
MINIMUM_FREE_RATIO = 0.01
IDENTITY = config.get('storage', 'identity-4')

os.makedirs(STORAGE_DIR, exist_ok=True)


def read_file_with_chunks(file_path, chunk_size):
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break


def get_hash(file_path):
    chunk_size = 1024 * 1024
    hasher = SHA3_256.new()
    for chunk in read_file_with_chunks(file_path, chunk_size):
        hasher.update(chunk)
    return hasher.hexdigest()


def get_file_path(uuid):
    return os.path.join(STORAGE_DIR, uuid)


def get_stored_bytes():
    stored_bytes = 0
    for dirpath, dirnames, filenames in os.walk(STORAGE_DIR):
        for filename in filenames:
            stored_bytes += os.path.getsize(os.path.join(dirpath, filename))
    return stored_bytes


def get_available_bytes():
    stored_bytes = get_stored_bytes()

    disk_usage = shutil.disk_usage(STORAGE_DIR)

    available_bytes = min([
        max([0, disk_usage.free - MINIMUM_FREE_MB * 1024 * 1024]),
        max([0, int(disk_usage.total * (1 - MINIMUM_FREE_RATIO) - stored_bytes)]),
    ])

    return available_bytes


@ctx_request.route(IDENTITY + '/file', methods=['POST'])
def create_file(request):
    uuid = request.data[b'uuid'].decode()
    content = request.data[b'content']
    file_path = get_file_path(uuid)

    available_bytes = get_available_bytes()

    if available_bytes > len(content):
        with open(file_path, 'wb') as f:
            f.write(content)
        file_hash = get_hash(file_path)
        status_code = requests.codes.ok
        available_bytes -= len(content)
    else:
        file_hash = ''
        status_code = requests.codes.forbidden

    return (
        {
            'uuid': uuid,
            'hash': file_hash,
            'available_bytes': available_bytes
        },
        status_code
    )


@ctx_request.route(IDENTITY + '/file', methods=['GET'], parameters=['uuid'])
def retrieve_file(request):
    uuid = request.parameters['uuid']
    file_path = get_file_path(uuid)

    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        status_code = requests.codes.ok
    except FileNotFoundError:
        content = ''
        status_code = requests.codes.not_found

    return (
        {
            'uuid': uuid,
            'content': content
        },
        status_code
    )


@ctx_request.route(IDENTITY + '/hash', methods=['GET'], parameters=['uuid'])
def retrieve_hash(request):
    uuid = request.parameters['uuid']
    file_path = get_file_path(uuid)

    try:
        file_hash = get_hash(file_path)
        status_code = requests.codes.ok
    except FileNotFoundError:
        file_hash = ''
        status_code = requests.codes.not_found

    return (
        {
            'uuid': uuid,
            'hash': file_hash
        },
        status_code
    )


@ctx_request.route(IDENTITY + '/file', methods=['DELETE'], parameters=['uuid'])
def delete_file(request):
    uuid = request.parameters['uuid']
    file_path = get_file_path(uuid)

    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass

    return {
        'uuid': uuid,
        'available_bytes': get_available_bytes(),
    }


@ctx_request.route(IDENTITY + '/stats', methods=['GET'])
def retrieve_stats(request):
    return {
        'available_bytes': get_available_bytes(),
        'stored_bytes': get_stored_bytes(),
    }


def run():
    response_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_response_hostname'),
                                        config.get('requests', 'worker_response_port'))
    control_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_control_hostname'),
                                       config.get('requests', 'worker_control_port'))

    worker = Worker(connect_response=response_url,
                    connect_control=control_url,
                    identity=IDENTITY)

    worker.run()


if __name__ == '__main__':
    run()
