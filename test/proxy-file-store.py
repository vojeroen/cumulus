import os
import random
import time
import uuid

import requests

from app.models.hub import Hub
from nimbus import config
from nimbus.client import Client

filename = 'learningreact1.pdf'
with open(os.path.join('tmp', filename), 'rb') as f:
    file_content = f.read() + str(random.randint(0, 100)).encode()

CONNECT_URL = 'tcp://{}:{}'.format(config.get('proxy-requests', 'client_hostname'),
                                   config.get('proxy-requests', 'client_port'))


def get_client():
    return Client(connect=CONNECT_URL, timeout=120)


hub = Hub.objects.first()
if hub is None:
    hub = Hub(reference='HUB-' + uuid.uuid4().hex)
    hub.save()

start = time.perf_counter()

print('Source: {}'.format(hub.cumulus_id))

print('Store file')
response = get_client().post(
    'file',
    parameters={'source': hub.cumulus_id,
                'collection': 'nextcloud',
                'name': filename},
    data=file_content
)

print('Read file')
response = get_client().get(
    'file',
    parameters={'source': hub.cumulus_id,
                'collection': 'nextcloud',
                'name': filename},
    decode_response=False
)

if not response.status_code == requests.codes.ok:
    print('Error in storage/retrieval (request)')
else:
    if not file_content == response.response[b'content']:
        print('Error in storage/retrieval (content)')

end = time.perf_counter()

print('elapsed time: {} s'.format(round(end - start, 3)))
