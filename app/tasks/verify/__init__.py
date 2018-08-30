from app.helpers import one
from app.models.file import File
from nimbus import config
from nimbus.log import get_logger

logger = get_logger(__name__)

VERIFY_FRACTION = float(config.get('verify', 'fraction'))


def v_all(func):
    files_to_reconstruct = list()

    for file in File.objects:
        if not getattr(file, func)():
            files_to_reconstruct.append(file.uuid)
            logger.debug('{} check failed: {}: {}/{}/{}'.format(
                func, file.uuid, file.source.cumulus_id, file.collection, file.filename
            ))

    logger.info('Files to reconstruct: {}'.format(len(files_to_reconstruct)))

    # for file_uuid in files_to_reconstruct:
    #     file = one(File.objects(uuid=file_uuid))
    #     file.reconstruct()


def v_random(func):
    pipeline = [{'$sample': {'size': int(len(File.objects) * VERIFY_FRACTION)}}]
    files_to_reconstruct = list()

    for file_dict in File.objects.aggregate(*pipeline):
        file = one(File.objects(uuid=file_dict['uuid']))
        if not getattr(file, func)():
            files_to_reconstruct.append(file.uuid)
            logger.debug('{} check failed: {}: {}/{}/{}'.format(
                func, file.uid, file.source, file.collection, file.filename
            ))

    logger.info('Files to reconstruct: {}'.format(len(files_to_reconstruct)))

    # for file_uuid in files_to_reconstruct:
    #     file = one(File.objects(uuid=file_uuid))
    #     file.reconstruct()
