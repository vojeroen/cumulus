from app.models.hub import Hub

for reference in range(3):
    hub = Hub(reference=str(reference))
    print(hub.reference, hub.id, hub.available_bytes)
    hub.save()
