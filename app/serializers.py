from nimbus.worker.serializer import Serializer

from app.models.file import File


class FileSerializer(Serializer):
    MODEL = File

    def serialize(self):
        return {
            'uuid': self.object.uuid,
            'timestamp_created': self.object.timestamp_created,
            'source': self.object.source.cumulus_id,
            'collection': self.object.collection.name,
            'name': self.object.filename,
            'hash': self.object.hash,
        }


class FileContentSerializer(FileSerializer):

    def serialize(self):
        data = super().serialize()
        with self.object as f:
            data['content'] = f.read()
        return data
