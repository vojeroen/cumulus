class HashError(ValueError):
    pass


class ReconstructionError(RuntimeError):
    pass


class NoRemoteStorageLocationFound(RuntimeError):
    pass


class RemoteStorageError(RuntimeError):
    pass


class InsufficientStorageSpace(RemoteStorageError):
    pass


class UploadFailed(RemoteStorageError):
    pass


class DownloadFailed(RemoteStorageError):
    pass


class DeleteFailed(RemoteStorageError):
    pass
