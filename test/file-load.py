from app.models.file import File

filename = 'fluentpython.pdf'
file = File.objects.filter(filename=filename).first()

with file as f:
    retrieved_content = f.read()

with open('tmp/fluentpython-retrieved.pdf', 'wb') as f:
    f.write(retrieved_content)
