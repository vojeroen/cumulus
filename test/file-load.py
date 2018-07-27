from app.models.file import File
from app.models.hub import Hub

hub = Hub.objects.first()

filename = 'fluentpython.pdf'
file = File.objects.filter(filename=filename).first()

with file as f:
    retrieved_content = f.read()

with open('tmp/fluentpython-retrieved.pdf', 'wb') as f:
    f.write(retrieved_content)
