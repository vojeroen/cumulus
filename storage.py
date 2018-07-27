import os

import requests
from Crypto.Hash import SHA3_256
from nimbus import config
from nimbus.worker.context import ctx_request
from nimbus.worker.worker import Worker

STORAGE_DIR = 'cache/storage'


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


@ctx_request.route('file', methods=['POST'])
def create_file(request):
    uuid = request.data[b'uuid'].decode()
    content = request.data[b'content']
    file_path = get_file_path(uuid)

    with open(file_path, 'wb') as f:
        f.write(content)

    return {
        'uuid': uuid,
        'hash': get_hash(file_path)
    }


@ctx_request.route('file', methods=['GET'], parameters=['uuid'])
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


@ctx_request.route('hash', methods=['GET'], parameters=['uuid'])
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


@ctx_request.route('file', methods=['DELETE'], parameters=['uuid'])
def delete_file(request):
    uuid = request.parameters['uuid']
    file_path = get_file_path(uuid)

    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass

    return {
        'uuid': uuid,
    }


if __name__ == '__main__':
    response_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_response_hostname'),
                                        config.get('requests', 'worker_response_port'))
    control_url = 'tcp://{}:{}'.format(config.get('requests', 'worker_control_hostname'),
                                       config.get('requests', 'worker_control_port'))

    identity = config.get('storage', 'identity')

    worker = Worker(connect_response=response_url,
                    connect_control=control_url,
                    identity=identity)

    worker.run()
