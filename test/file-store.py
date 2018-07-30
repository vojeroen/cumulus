import os
import time
import uuid

from app.models.file import File, Collection, Encoding
from app.models.hub import Hub

filename = 'fluentpython.pdf'
with open(os.path.join('tmp', filename), 'rb') as f:
    file_content = f.read()

hub = Hub.objects.first()
if hub is None:
    hub = Hub(reference='HUB-' + uuid.uuid4().hex)
    hub.save()

start = time.perf_counter()

file = File()
file.source = hub
file.collection = Collection(name='nextcloud')
file.filename = filename
file.encoding = Encoding(name='liberasurecode_rs_vand', k=2, m=3)

print('Store file')
with file as f:
    f.write(file_content)

print('Open file without action')
with file as f:
    pass

print('Read file')
with file as f:
    retrieved_content = f.read()

with open('tmp/fluentpython-retrieved.pdf', 'wb') as f:
    f.write(retrieved_content)

if not file_content == retrieved_content:
    print('Error in storage/retrieval')

end = time.perf_counter()

print('elapsed time: {} s'.format(round(end - start, 3)))
