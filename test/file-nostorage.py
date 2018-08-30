import os
import time
import uuid

from app.models.error import NoRemoteStorageLocationFound
from app.models.file import File, Encoding
from app.models.fragment import OrphanedFragment
from app.models.hub import Hub

filename1 = 'fluentpython.pdf'
with open(os.path.join('tmp', filename1), 'rb') as f:
    file_content1 = f.read()

filename2 = 'learningreact1.pdf'
with open(os.path.join('tmp', filename2), 'rb') as f:
    file_content2 = f.read()

hub = Hub.objects.first()
if hub is None:
    hub = Hub(reference='HUB-' + uuid.uuid4().hex)
    hub.save()

start = time.perf_counter()

file = File()
file.source = hub
file.collection = 'nextcloud'
file.filename = filename1
file.encoding = Encoding(name='liberasurecode_rs_vand', k=2, m=3)

with file as f:
    f.write(file_content1)

for t_hub in Hub.objects:
    if t_hub.cumulus_id != hub.cumulus_id:
        t_hub.delete()

orphan_count = OrphanedFragment.objects.count()
fragments = set([f.uuid for f in file.fragments])

try:
    with file as f:
        f.write(file_content2)
except NoRemoteStorageLocationFound:
    pass

file.reload()

if fragments != set([f.uuid for f in file.fragments]):
    print('Error: different fragments')

if orphan_count != OrphanedFragment.objects.count():
    print('Error: new orphans')

end = time.perf_counter()

print('elapsed time: {} s'.format(round(end - start, 3)))
