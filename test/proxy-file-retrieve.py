import os
import time

import requests

from app.models.hub import Hub
from nimbus import config
from nimbus.client import Client

filename = 'learningreact1.pdf'
with open(os.path.join('tmp', filename), 'rb') as f:
    file_content = f.read()

CONNECT_URL = 'tcp://{}:{}'.format(config.get('proxy-requests', 'client_hostname'),
                                   config.get('proxy-requests', 'client_port'))


def get_client():
    return Client(connect=CONNECT_URL, timeout=120)


hub = Hub.objects.first()

start = time.perf_counter()

print('Source: {}'.format(hub.cumulus_id))

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

end = time.perf_counter()

print('elapsed time: {} s'.format(round(end - start, 3)))
