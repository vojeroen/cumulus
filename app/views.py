from mongoengine import Q
from nimbus.worker.context import ctx_request
from nimbus.worker.errors import MultipleObjectsFound, ObjectDoesNotExist

from app.models.file import File, Collection, Encoding
from app.models.hub import Hub
from app.serializers import FileSerializer, FileContentSerializer

DEFAULT_ENCODING = {
    'name': 'liberasurecode_rs_vand',
    'k': 2,
    'm': 3,
}


@ctx_request.route('file', methods=['LIST'])
def list_files(request):
    files = File.objects
    if 'source' in request.parameters:
        files = files(source=request.parameters['source'])
    return FileSerializer(files, list_allowed=True).data


@ctx_request.route('file', methods=['POST'],
                   parameters=['source', 'collection', 'name'])
def post_file(request):
    files = File.objects(Q(source=request.parameters['source']) &
                         Q(collection__name=request.parameters['collection']) &
                         Q(filename=request.parameters['name'])).all()
    if len(files) == 0:
        file = File()
        file.collection = Collection(name=request.parameters['collection'])
        file.filename = request.parameters['name']
        file.encoding = Encoding(**DEFAULT_ENCODING)
        hubs = Hub.objects(cumulus_id=request.parameters['source'])
        if len(hubs) == 0:
            raise ObjectDoesNotExist('Source does not exist')
        elif len(hubs) == 1:
            hub = hubs[0]
        else:
            raise MultipleObjectsFound('Multiple files found for source')
        file.source = hub
    elif len(files) == 1:
        file = files[0]
    else:
        raise MultipleObjectsFound('Multiple objects found for the search query')

    with file as f:
        f.write(request.data)

    return FileSerializer(file).data


@ctx_request.route('file', methods=['GET'],
                   parameters=['source', 'collection', 'name'])
def get_file(request):
    files = File.objects(Q(source=request.parameters['source']) &
                         Q(collection__name=request.parameters['collection']) &
                         Q(filename=request.parameters['name'])).all()
    if len(files) == 0:
        raise ObjectDoesNotExist('File does not exist')
    elif len(files) == 1:
        file = files[0]
    else:
        raise MultipleObjectsFound('Multiple files found for the search query')

    return FileContentSerializer(file).data
