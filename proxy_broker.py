from nimbus import config
from nimbus.broker import Broker

zmq_worker_response_url = 'tcp://{}:{}'.format('*', config.get('proxy-requests', 'worker_response_port'))
zmq_worker_control_url = 'tcp://{}:{}'.format('*', config.get('proxy-requests', 'worker_control_port'))
zmq_client_url = 'tcp://{}:{}'.format('*', config.get('proxy-requests', 'client_port'))

broker = Broker(worker_response_bind=zmq_worker_response_url,
                worker_control_bind=zmq_worker_control_url,
                client_bind=zmq_client_url)

broker.run()
