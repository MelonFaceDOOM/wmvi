from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.exceptions import ContentDecodingError

from ingestion.podcast import PodcastShowRow
from services.podcast.monitor import monitor as mon


def _show(**kwargs) -> PodcastShowRow:
    defaults = {
        "id": 46,
        "title": "Test Show",
        "rss_url": "https://example.com/feed.xml",
        "etag": '"etag-1"',
        "last_modified": "Thu, 05 Jun 2026 12:00:00 GMT",
    }
    defaults.update(kwargs)
    return PodcastShowRow(**defaults)


def test_format_show_fetch_error_includes_url_and_response_metadata() -> None:
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {"Content-Encoding": "br"}
    exc = ContentDecodingError("brotli decode failed", response=resp)

    detail = mon.format_show_fetch_error(_show(), exc)

    assert "ContentDecodingError" in detail
    assert "url=https://example.com/feed.xml" in detail
    assert "status=200" in detail
    assert "content-encoding=br" in detail


@patch("services.podcast.monitor.monitor.requests.get")
def test_fetch_rss_retries_decode_failure_with_simpler_encoding(mock_get) -> None:
    decode_error = ContentDecodingError("brotli decode failed")
    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.text = "<rss></rss>"
    ok_resp.headers = {"ETag": '"etag-2"', "Last-Modified": "Thu, 05 Jun 2026 12:01:00 GMT"}
    ok_resp.raise_for_status = MagicMock()
    mock_get.side_effect = [decode_error, ok_resp]

    status, rss_text, etag, last_modified = mon.fetch_rss(_show())

    assert status == 200
    assert rss_text == "<rss></rss>"
    assert etag == '"etag-2"'
    assert last_modified == "Thu, 05 Jun 2026 12:01:00 GMT"
    assert mock_get.call_count == 2
    assert "Accept-Encoding" not in mock_get.call_args_list[0].kwargs["headers"]
    assert mock_get.call_args_list[1].kwargs["headers"]["Accept-Encoding"] == "gzip, deflate"


@patch("services.podcast.monitor.monitor.requests.get")
def test_fetch_rss_raises_after_exhausting_decode_retries(mock_get) -> None:
    mock_get.side_effect = ContentDecodingError("still broken")

    with pytest.raises(ContentDecodingError, match="still broken"):
        mon.fetch_rss(_show())

    assert mock_get.call_count == len(mon.RSS_ACCEPT_ENCODING_STRATEGIES)


@patch("services.podcast.monitor.monitor.requests.get")
def test_fetch_rss_preserves_conditional_headers(mock_get) -> None:
    resp = MagicMock()
    resp.status_code = 304
    resp.text = ""
    resp.headers = {}
    mock_get.return_value = resp

    status, rss_text, etag, last_modified = mon.fetch_rss(_show())

    assert status == 304
    assert rss_text is None
    headers = mock_get.call_args.kwargs["headers"]
    assert headers["If-None-Match"] == '"etag-1"'
    assert headers["If-Modified-Since"] == "Thu, 05 Jun 2026 12:00:00 GMT"
