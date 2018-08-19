from nimbus import config
from nimbus.worker.worker import Worker

from app import views

response_url = 'tcp://{}:{}'.format(config.get('proxy-requests', 'worker_response_hostname'),
                                    config.get('proxy-requests', 'worker_response_port'))
control_url = 'tcp://{}:{}'.format(config.get('proxy-requests', 'worker_control_hostname'),
                                   config.get('proxy-requests', 'worker_control_port'))

worker = Worker(connect_response=response_url,
                connect_control=control_url)
