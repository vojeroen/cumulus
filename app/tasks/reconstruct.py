#!/usr/bin/env python3
import os

from app.models.file import File
from nimbus.log import get_logger

LOCK_FILE = '/tmp/cumulus/reconstruct.lock'
logger = get_logger(__name__)


def reconstruct_files():
    count = 0
    for file in File.objects(fragments__is_clean=False):
        logger.debug('Reconstructing {}'.format(file))
        file.reconstruct()
        count += 1
    return count


if __name__ == '__main__':
    os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
    if not os.path.exists(LOCK_FILE):
        logger.info('Starting file reconstruction')
        open(LOCK_FILE, 'wb').close()
        count = reconstruct_files()
        os.remove(LOCK_FILE)
        logger.info('Finished file reconstruction. Reconstructed files: {}'.format(count))
    else:
        logger.info('Another file reconstruction is running, aborting...')
