from app import views
from nimbus import config
from nimbus.worker.worker import Worker

views._prevent_pycharm_from_removing_import = True

response_url = 'tcp://{}:{}'.format(config.get('proxy-requests', 'worker_response_hostname'),
                                    config.get('proxy-requests', 'worker_response_port'))
control_url = 'tcp://{}:{}'.format(config.get('proxy-requests', 'worker_control_hostname'),
                                   config.get('proxy-requests', 'worker_control_port'))

worker = Worker(connect_response=response_url,
                connect_control=control_url)
