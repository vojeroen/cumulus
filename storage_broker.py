from nimbus import config
from nimbus.broker import Broker, InvalidEndpoint, BrokerSecurityManager
from nimbus.helpers.message import decode


def validate_endpoints(worker_id, endpoints):
    for endpoint in endpoints:
        if endpoint[:len(worker_id) + 1] != decode(worker_id) + '/':
            raise InvalidEndpoint
    return endpoints


broker = Broker(
    worker_response_bind='tcp://{}:{}'.format('*', config.get('storage-requests', 'worker_response_port')),
    worker_control_bind='tcp://{}:{}'.format('*', config.get('storage-requests', 'worker_control_port')),
    client_bind='tcp://{}:{}'.format('*', config.get('storage-requests', 'client_port')),
    validate_endpoints=validate_endpoints,
    security_manager=BrokerSecurityManager(
        connection_secret_key=config.get('security', 'connection_secret_key'),
        connection_public_keys=config.get('security', 'connection_public_keys'),
        message_secret_key=config.get('security', 'message_secret_key'),
        message_public_keys=config.get('security', 'message_public_keys')
    )
)

broker.run()
