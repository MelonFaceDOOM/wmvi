"""
Upload a local file to Azure Blob Storage.

Reads credentials from environment variables (or a .env file):
  AZURE_STORAGE_ACCOUNT   - storage account name
  AZURE_STORAGE_KEY       - base64-encoded account key
  AZURE_STORAGE_CONTAINER - target container name

Usage:
  python scripts/azure_upload.py <local_file> [--dest <blob_path>]

If --dest is omitted the blob name defaults to the local filename.
"""

from __future__ import annotations

import argparse
import mimetypes
import sys
from pathlib import Path

from dotenv import load_dotenv

from services.storage import AzureBlobStorage


load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a file to Azure Blob Storage.")
    parser.add_argument("file", help="Path to the local file to upload.")
    parser.add_argument(
        "--dest",
        default=None,
        help="Blob destination path inside the container (default: filename).",
    )
    args = parser.parse_args()

    src = Path(args.file)
    if not src.is_file():
        print(f"error: {src} is not a file", file=sys.stderr)
        sys.exit(1)

    dest = args.dest or src.name

    storage = AzureBlobStorage.from_env()

    ok, reason = storage.is_accessible()
    if not ok:
        print(f"error: storage not accessible: {reason}", file=sys.stderr)
        sys.exit(1)

    data = src.read_bytes()
    content_type, _ = mimetypes.guess_type(src.name)
    content_type = content_type or "application/octet-stream"

    print(f"Uploading {src} → {storage.container}/{dest} ({len(data)} bytes, {content_type})")
    storage.write_bytes(dest, data, content_type=content_type)
    print("Done.")


if __name__ == "__main__":
    main()
