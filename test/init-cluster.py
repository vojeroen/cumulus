import configparser
import os
import shutil
from subprocess import call

import zmq.auth
from Crypto.PublicKey import DSA
from jinja2 import Template

from app.models.file import File
from app.models.fragment import OrphanedFragment
from app.models.hub import Hub

CLUSTER_DIR = 'cluster'
NUM_STORAGE_WORKERS = 5
DSA_KEY_SIZE = 1024  # at least 2048 for production environments

################
# stop cluster #
################

if os.path.exists('cluster/cluster-stop.sh'):
    call(['./cluster-stop.sh'], cwd='cluster')

############
# clean up #
############

# remove existing files
try:
    shutil.rmtree(CLUSTER_DIR)
except FileNotFoundError:
    pass

# clean database
for file in File.objects:
    file.delete()

for orphan in OrphanedFragment.objects:
    orphan.delete()

for hub in Hub.objects:
    hub.delete()

###############
# prepare new #
###############

# create new hubs
new_hubs = []
for reference in range(NUM_STORAGE_WORKERS + int(NUM_STORAGE_WORKERS * 0.4)):
    hub = Hub(reference=str(reference))
    hub.save()
    print(hub.reference, hub.id, hub.available_bytes)
    new_hubs.append(hub.cumulus_id)

# create directory structure
os.makedirs(CLUSTER_DIR, exist_ok=True)

#########################
# set up storage broker #
#########################

# directories
for d in [os.path.join(CLUSTER_DIR, 'storage-broker'),
          os.path.join(CLUSTER_DIR, 'storage-broker/keys/connection-private'),
          os.path.join(CLUSTER_DIR, 'storage-broker/keys/connection-public'),
          os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-private'),
          os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-public'),
          ]:
    os.makedirs(d, exist_ok=True)

# files
shutil.copy('sample_configuration_storage_broker',
            os.path.join(CLUSTER_DIR, 'storage-broker', 'configuration'))
shutil.copy('storage_broker.py',
            os.path.join(CLUSTER_DIR, 'storage-broker', 'storage_broker.py'))

# connection certificates
zmq.auth.create_certificates(os.path.join(CLUSTER_DIR, 'storage-broker/keys/connection-private'),
                             'storage-broker')

# message validation certificates
key = DSA.generate(DSA_KEY_SIZE)
with open(os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-private', 'storage-broker.secret.pem'), 'wb') as ofile:
    ofile.write(key.export_key('PEM'))
with open(os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-private', 'storage-broker.pem'), 'wb') as ofile:
    ofile.write(key.publickey().export_key('PEM'))

##########################
# set up storage workers #
##########################

for i in range(NUM_STORAGE_WORKERS):
    storage_worker_dir = 'storage-worker-{}'.format(i)

    # directories
    for d in [os.path.join(CLUSTER_DIR, storage_worker_dir),
              os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/connection-private'),
              os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/connection-public'),
              os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/message-private'),
              os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/message-public'),
              ]:
        os.makedirs(d, exist_ok=True)

    # files
    config = configparser.ConfigParser()
    config.read('sample_configuration_storage_worker')
    config['storage']['identity'] = new_hubs[i]
    with open(os.path.join(CLUSTER_DIR, storage_worker_dir, 'configuration'), 'w') as ofile:
        config.write(ofile)
    shutil.copy('storage_worker.py',
                os.path.join(CLUSTER_DIR, storage_worker_dir, 'storage_worker.py'))

    # connection certificates
    zmq.auth.create_certificates(os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/connection-private'),
                                 'storage-worker')

    # copy connection certificates
    shutil.copy(os.path.join(CLUSTER_DIR, 'storage-broker/keys/connection-private/storage-broker.key'),
                os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/connection-public', 'storage-broker.key'))
    shutil.copy(os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/connection-private/storage-worker.key'),
                os.path.join(CLUSTER_DIR, 'storage-broker/keys/connection-public', new_hubs[i].lower() + '.key'))

    # message validation certificates
    key = DSA.generate(DSA_KEY_SIZE)
    with open(os.path.join(CLUSTER_DIR,
                           storage_worker_dir,
                           'keys/message-private', 'storage-worker.secret.pem'), 'wb') as ofile:
        ofile.write(key.export_key('PEM'))
    with open(os.path.join(CLUSTER_DIR,
                           storage_worker_dir,
                           'keys/message-private', 'storage-worker.pem'), 'wb') as ofile:
        ofile.write(key.publickey().export_key('PEM'))

    # copy message validation certificates
    shutil.copy(os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-private/storage-broker.pem'),
                os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/message-public', 'broker.pem'))
    shutil.copy(os.path.join(CLUSTER_DIR, storage_worker_dir, 'keys/message-private/storage-worker.pem'),
                os.path.join(CLUSTER_DIR, 'storage-broker/keys/message-public', new_hubs[i].lower() + '.pem'))

#######################
# set up proxy broker #
#######################

os.makedirs(os.path.join(CLUSTER_DIR, 'proxy-broker'), exist_ok=True)
shutil.copy('proxy_broker.py',
            os.path.join(CLUSTER_DIR, 'proxy-broker', 'proxy_broker.py'))
shutil.copy('sample_configuration_proxy_broker',
            os.path.join(CLUSTER_DIR, 'proxy-broker', 'configuration'))

#######################
# set up proxy worker #
#######################

os.makedirs(os.path.join(CLUSTER_DIR, 'proxy-worker'), exist_ok=True)
shutil.copy('proxy_worker.py',
            os.path.join(CLUSTER_DIR, 'proxy-worker', 'proxy_worker.py'))
shutil.copy('sample_configuration_proxy_worker',
            os.path.join(CLUSTER_DIR, 'proxy-worker', 'configuration'))

####################################
# bash script to launch everything #
####################################

script = """
#!/bin/bash

export PYTHONPATH={{dirs}}:$PYTHONPATH
CLUSTER_DIR=`pwd`

source {{virtualenv}}

{% for dir, file in files %}
cd ${CLUSTER_DIR}/{{dir}}
python {{file}} >/dev/null & 
echo $! > pid
{% endfor %}
cd ${CLUSTER_DIR}
"""
with open('cluster/cluster-start.sh', 'wb') as ofile:
    ofile.write(Template(script.strip()).render(
        virtualenv='~/virtualenv/cumulus/bin/activate',
        dirs=':'.join([os.getcwd(), os.path.join(os.getcwd(), '../nimbus')]),
        files=[
                  ('proxy-broker', 'proxy_broker.py'),
                  ('proxy-worker', 'proxy_worker.py'),
                  ('storage-broker', 'storage_broker.py'),
              ] + [
                  ('storage-worker-{i}'.format(i=i), 'storage_worker.py') for i in range(NUM_STORAGE_WORKERS)
              ]
    ).encode())
os.chmod('cluster/cluster-start.sh', 0o755)

####################################
# bash script to stop everything #
####################################

script = """
#!/bin/bash

CLUSTER_DIR=`pwd`

{% for dir, file in files %}
cd ${CLUSTER_DIR}/{{dir}}
if [ -f pid ]; then
    kill $(cat pid) || true
    rm pid
fi
{% endfor %}
cd ${CLUSTER_DIR}
"""
with open('cluster/cluster-stop.sh', 'wb') as ofile:
    ofile.write(Template(script.strip()).render(
        files=[
                  ('proxy-broker', 'proxy_broker.py'),
                  ('proxy-worker', 'proxy_worker.py'),
                  ('storage-broker', 'storage_broker.py'),
              ] + [
                  ('storage-worker-{i}'.format(i=i), 'storage_worker.py') for i in range(NUM_STORAGE_WORKERS)
              ]
    ).encode())
os.chmod('cluster/cluster-stop.sh', 0o755)

####################################
# bash script to stop everything #
####################################

script = """
#!/bin/bash

./cluster-stop.sh
./cluster-start.sh
"""
with open('cluster/cluster-restart.sh', 'wb') as ofile:
    ofile.write(Template(script.strip()).render(
        files=[
                  ('proxy-broker', 'proxy_broker.py'),
                  ('proxy-worker', 'proxy_worker.py'),
                  ('storage-broker', 'storage_broker.py'),
              ] + [
                  ('storage-worker-{i}'.format(i=i), 'storage_worker.py') for i in range(NUM_STORAGE_WORKERS)
              ]
    ).encode())
os.chmod('cluster/cluster-restart.sh', 0o755)


#################
# start cluster #
#################

if os.path.exists('cluster/cluster-start.sh'):
    call(['./cluster-start.sh'], cwd='cluster')
