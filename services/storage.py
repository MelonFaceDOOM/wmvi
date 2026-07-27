""" Backward-compatible re-exports; prefer `storage.backends` for new code."""

from storage.backends import (
    AzureBlobStorage,
    LocalFileStorage,
    NitwitchUploadStorage,
    StorageBackend,
)

__all__ = [
    "AzureBlobStorage",
    "LocalFileStorage",
    "NitwitchUploadStorage",
    "StorageBackend",
]
