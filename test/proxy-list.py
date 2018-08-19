from pprint import pprint

from nimbus import config
from nimbus.client import Client

CONNECT_URL = 'tcp://{}:{}'.format(config.get('proxy-requests', 'client_hostname'),
                                   config.get('proxy-requests', 'client_port'))

client = Client(connect=CONNECT_URL)
response = client.list('file').response
pprint(response)

source = response[0]['source']

response = client.list('file', parameters={'source': source}).response
pprint(response)

response = client.list('file', parameters={'source': 'non-existing-id'}).response
pprint(response)
