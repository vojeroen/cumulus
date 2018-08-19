from nimbus import config
from nimbus.broker import Broker, InvalidEndpoint
from nimbus.helpers.message import decode

zmq_worker_response_url = 'tcp://{}:{}'.format('*', config.get('storage-requests', 'worker_response_port'))
zmq_worker_control_url = 'tcp://{}:{}'.format('*', config.get('storage-requests', 'worker_control_port'))
zmq_client_url = 'tcp://{}:{}'.format('*', config.get('storage-requests', 'client_port'))

redis_host = config.get('redis', 'host')
redis_port = config.get('redis', 'port')
redis_db = config.get('redis', 'db')


# broker = Broker(worker_response_bind=zmq_worker_response_url,
#                 worker_control_bind=zmq_worker_control_url,
#                 client_bind=zmq_client_url,
#                 redis_host=redis_host,
#                 redis_port=redis_port,
#                 redis_db=redis_db)

def validate_endpoints(worker_id, endpoints):
    for endpoint in endpoints:
        if endpoint[:len(worker_id) + 1] != decode(worker_id) + '/':
            raise InvalidEndpoint
    return endpoints


broker = Broker(worker_response_bind=zmq_worker_response_url,
                worker_control_bind=zmq_worker_control_url,
                client_bind=zmq_client_url,
                validate_endpoints=validate_endpoints)

broker.run()
