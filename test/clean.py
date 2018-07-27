from app.models.file import File
from app.models.fragment import OrphanedFragment
from app.models.hub import Hub

for file in File.objects:
    file.remove()

for hub in Hub.objects:
    hub.delete()

for orphan in OrphanedFragment.objects:
    orphan.remove()
