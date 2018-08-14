from nimbus.config import cparser, CONFIG_FILE_PATH

from app.models.hub import Hub

for reference in range(7):
    hub = Hub(reference=str(reference))
    print(hub.reference, hub.id, hub.available_bytes)
    if reference == 0:
        cparser['storage']['identity'] = hub.cumulus_id
    else:
        cparser['storage']['identity-{}'.format(reference + 1)] = hub.cumulus_id
    hub.save()

with open(CONFIG_FILE_PATH, 'w') as ofile:
    cparser.write(ofile)
