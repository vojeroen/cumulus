import os
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

file = File()
file.source = hub
file.collection = Collection(name='nextcloud')
file.filename = filename
file.encoding = Encoding(name='liberasurecode_rs_vand', k=2, m=3)

with file as f:
    f.write(file_content)

with file as f:
    retrieved_content = f.read()

with open('tmp/fluentpython-retrieved.pdf', 'wb') as f:
    f.write(retrieved_content)

if not file_content == retrieved_content:
    print('Error in storage/retrieval')
