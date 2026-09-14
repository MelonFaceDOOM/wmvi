from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from telethon import errors

from services.telegram_monitor.tg_scrape import (
    ChannelState,
    ScrapePass,
    apply_backfill,
    apply_catch_up,
    flood_exceeds_cap,
    flood_sleep_s,
    scrape_channel_batches,
)


def _dt(hour: int) -> datetime:
    return datetime(2026, 9, 14, hour, tzinfo=timezone.utc)


def _msg(msg_id: int, hour: int, text: str = "hello"):
    return SimpleNamespace(
        id=msg_id,
        date=_dt(hour),
        message=text,
        media=None,
        pinned=False,
        views=None,
        forwards=None,
        replies=None,
        reactions=None,
    )


def _flood(seconds: int) -> errors.FloodWaitError:
    try:
        return errors.FloodWaitError(None, seconds)
    except TypeError:
        err = errors.FloodWaitError.__new__(errors.FloodWaitError)
        err.seconds = seconds
        return err


class FakeClient:
    def __init__(self, messages, flood_after: int | None = None, flood_s: int = 5):
        self.messages = list(messages)
        self.flood_after = flood_after
        self.flood_s = flood_s
        self.kwargs = None

    async def iter_messages(self, entity, **kwargs):
        self.kwargs = kwargs
        emitted = 0
        min_id = kwargs.get("min_id") or 0
        max_id = kwargs.get("max_id") or 0
        for msg in self.messages:
            if min_id and msg.id <= min_id:
                continue
            if max_id and msg.id >= max_id:
                continue
            if self.flood_after is not None and emitted >= self.flood_after:
                raise _flood(self.flood_s)
            emitted += 1
            yield msg


def test_flood_cap_skips_long_waits() -> None:
    short = _flood(10)
    long = _flood(35863)
    assert flood_sleep_s(short) == 11
    assert not flood_exceeds_cap(short)
    assert flood_exceeds_cap(long)


def test_catch_up_does_not_advance_high_after_flood() -> None:
    state = ChannelState(channel_id=1, high_watermark_id=10, high_watermark_ts=_dt(1))
    progress = ScrapePass(
        reason="flood", flood_seconds=30, newest_id=50, newest_ts=_dt(5),
    )
    apply_catch_up(state, progress)
    assert state.high_watermark_id == 10


def test_catch_up_advances_high_only_when_contiguous() -> None:
    state = ChannelState(channel_id=1, high_watermark_id=10, high_watermark_ts=_dt(1))
    progress = ScrapePass(reason="caught_up", newest_id=50, newest_ts=_dt(5))
    apply_catch_up(state, progress)
    assert state.high_watermark_id == 50
    assert state.high_watermark_ts == _dt(5)


def test_backfill_sets_high_on_first_scrape_and_moves_low() -> None:
    state = ChannelState(channel_id=1)
    flooded = ScrapePass(
        reason="flood",
        newest_id=100,
        newest_ts=_dt(10),
        oldest_id=80,
        oldest_ts=_dt(8),
    )
    apply_backfill(state, flooded)
    assert state.high_watermark_id == 100
    assert state.low_watermark_id == 80
    assert state.backfill_complete is False

    continued = ScrapePass(
        reason="history_end",
        newest_id=79,
        newest_ts=_dt(7),
        oldest_id=1,
        oldest_ts=_dt(0),
    )
    apply_backfill(state, continued)
    assert state.high_watermark_id == 100
    assert state.low_watermark_id == 1
    assert state.backfill_complete is True


def test_scrape_pass_catch_up_uses_min_id() -> None:
    async def _run():
        msgs = [_msg(i, i, f"t{i}") for i in range(5, 0, -1)]
        client = FakeClient(msgs)
        entity = SimpleNamespace(id=99, username="demo", access_hash=123)
        progress = ScrapePass()
        batches = []
        async for batch in scrape_channel_batches(
            client, "@demo", entity=entity, min_id=3, batch_size=10, progress=progress,
        ):
            batches.append(batch)

        assert client.kwargs["min_id"] == 3
        ids = [row["message_id"] for row in batches[0]]
        assert ids == [5, 4]
        assert progress.reason == "caught_up"
        assert progress.newest_id == 5
        assert progress.oldest_id == 4

    asyncio.run(_run())


def test_scrape_pass_backfill_uses_max_id() -> None:
    async def _run():
        msgs = [_msg(i, i, f"t{i}") for i in range(5, 0, -1)]
        client = FakeClient(msgs)
        entity = SimpleNamespace(id=99, username="demo", access_hash=123)
        progress = ScrapePass()
        batches = []
        async for batch in scrape_channel_batches(
            client, "@demo", entity=entity, max_id=4, batch_size=10, progress=progress,
        ):
            batches.append(batch)

        ids = [row["message_id"] for row in batches[0]]
        assert ids == [3, 2, 1]
        assert progress.reason == "history_end"
        assert progress.oldest_id == 1

    asyncio.run(_run())


def test_scrape_pass_flood_yields_buffer_and_does_not_sleep_long(monkeypatch) -> None:
    async def _run():
        msgs = [_msg(i, i, f"t{i}") for i in range(5, 0, -1)]
        client = FakeClient(msgs, flood_after=2, flood_s=40000)
        entity = SimpleNamespace(id=99, username="demo", access_hash=123)
        slept = []

        async def _no_sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr(
            "services.telegram_monitor.tg_scrape.asyncio.sleep", _no_sleep,
        )

        progress = ScrapePass()
        batches = []
        async for batch in scrape_channel_batches(
            client, "@demo", entity=entity, batch_size=10, progress=progress,
        ):
            batches.append(batch)

        assert progress.reason == "flood"
        assert progress.flood_seconds == 40000
        assert progress.messages_seen == 2
        assert slept == []  # over cap, skip
        assert [row["message_id"] for row in batches[0]] == [5, 4]

    asyncio.run(_run())


def test_empty_text_still_moves_watermarks() -> None:
    async def _run():
        msgs = [
            _msg(5, 5, "keep"),
            _msg(4, 4, ""),
            _msg(3, 3, "  "),
            _msg(2, 2, "also"),
        ]
        client = FakeClient(msgs)
        entity = SimpleNamespace(id=99, username="demo", access_hash=123)
        progress = ScrapePass()
        batches = []
        async for batch in scrape_channel_batches(
            client, "@demo", entity=entity, batch_size=10, progress=progress,
        ):
            batches.append(batch)

        ids = [row["message_id"] for row in batches[0]]
        assert ids == [5, 2]
        assert progress.newest_id == 5
        assert progress.oldest_id == 2
        assert progress.messages_seen == 4
        assert progress.messages_with_text == 2
        assert progress.reason == "history_end"

    asyncio.run(_run())
