from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import os
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class StorageBackend(ABC):
    @abstractmethod
    def is_accessible(self) -> tuple[bool, Optional[str]]:
        raise NotImplementedError

    @abstractmethod
    def write_text(self, rel_path: str, text: str) -> None:
        raise NotImplementedError


class LocalFileStorage(StorageBackend):
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            test_path = self.base_dir / ".write_test"
            test_path.write_text("ok", encoding="utf-8")
            test_path.unlink()
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        dest = self.base_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")


class AzureBlobStorage(StorageBackend):
    # Pinned Azure Storage REST API version for Shared Key requests.
    # Deliberately fixed to a tested version; change only with re-testing.
    API_VERSION = "2023-11-03"

    def __init__(
        self,
        account: str,
        account_key: str,
        container: str,
    ) -> None:
        self.account = account.strip()
        self.account_key = account_key.strip()
        self.container = container.strip()

    @classmethod
    def from_env(cls) -> "AzureBlobStorage":
        return cls(
            account=os.environ["AZURE_STORAGE_ACCOUNT"],
            account_key=os.environ["AZURE_STORAGE_KEY"],
            container=os.environ["AZURE_STORAGE_CONTAINER"],
        )

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            url = self._container_url(restype="container")
            req = self._build_request(url=url, method="GET")
            with urllib.request.urlopen(req, timeout=30) as resp:
                if 200 <= resp.status < 300:
                    return True, None
                return False, f"Unexpected HTTP status: {resp.status}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        blob_name = self._full_blob_name(rel_path)
        url = self._blob_url(blob_name)

        body = text.encode("utf-8")
        content_length = str(len(body))
        content_type = "application/json; charset=utf-8"

        req = self._build_request(
            url=url,
            method="PUT",
            content_length=content_length,
            content_type=content_type,
            extra_headers={"x-ms-blob-type": "BlockBlob"},
            body=body,
        )

        with urllib.request.urlopen(req, timeout=60) as resp:
            if not (200 <= resp.status < 300):
                raise RuntimeError(f"Blob upload failed with HTTP {resp.status}")

    def write_bytes(
        self,
        rel_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        blob_name = self._full_blob_name(rel_path)
        url = self._blob_url(blob_name)

        req = self._build_request(
            url=url,
            method="PUT",
            content_length=str(len(data)),
            content_type=content_type,
            extra_headers={"x-ms-blob-type": "BlockBlob"},
            body=data,
        )

        with urllib.request.urlopen(req, timeout=60) as resp:
            if not (200 <= resp.status < 300):
                raise RuntimeError(f"Blob upload failed with HTTP {resp.status}")

    def _full_blob_name(self, rel_path: str) -> str:
        clean = rel_path.lstrip("/")
        return clean

    def _container_url(self, *, restype: str) -> str:
        return (
            f"https://{self.account}.blob.core.windows.net/"
            f"{self.container}?restype={restype}"
        )

    def _blob_url(self, blob_name: str) -> str:
        return (
            f"https://{self.account}.blob.core.windows.net/"
            f"{self.container}/{blob_name}"
        )

    def _build_request(
        self,
        *,
        url: str,
        method: str,
        content_length: str = "",
        content_type: str = "",
        extra_headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> urllib.request.Request:
        x_ms_date = dt.datetime.now(dt.timezone.utc).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )

        headers = {
            "x-ms-date": x_ms_date,
            "x-ms-version": self.API_VERSION,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if extra_headers:
            headers.update(extra_headers)

        auth = self._build_auth_header(
            method=method,
            url=url,
            content_length=content_length,
            content_type=content_type,
            headers=headers,
        )

        req = urllib.request.Request(url, data=body, method=method)
        for k, v in headers.items():
            req.add_header(k, v)
        req.add_header("Authorization", auth)
        if content_length:
            req.add_header("Content-Length", content_length)
        return req

    def _build_auth_header(
        self,
        *,
        method: str,
        url: str,
        content_length: str,
        content_type: str,
        headers: dict[str, str],
    ) -> str:
        canonicalized_headers = self._canonicalized_headers(headers)
        canonicalized_resource = self._canonicalized_resource(url)

        string_to_sign = (
            f"{method}\n"
            f"\n"
            f"\n"
            f"{content_length if content_length and content_length != '0' else ''}\n"
            f"\n"
            f"{content_type}\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"\n"
            f"{canonicalized_headers}"
            f"{canonicalized_resource}"
        )

        key_bytes = base64.b64decode(self.account_key)
        sig = base64.b64encode(
            hmac.new(key_bytes, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")

        return f"SharedKey {self.account}:{sig}"

    def _canonicalized_headers(self, headers: dict[str, str]) -> str:
        x_ms_headers = {
            k.lower(): " ".join(v.strip().split())
            for k, v in headers.items()
            if k.lower().startswith("x-ms-")
        }
        return "".join(f"{k}:{x_ms_headers[k]}\n" for k in sorted(x_ms_headers))

    def _canonicalized_resource(self, url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        out = f"/{self.account}{parsed.path}"

        if parsed.query:
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            for key in sorted(k.lower() for k in params):
                vals = params[key]
                out += f"\n{key}:{','.join(sorted(vals))}"
        return out
