from services.cli.install import timer_triggers
from services.cli.lib.config import TimerConfig


def test_timer_triggers_interval() -> None:
    t = TimerConfig(on_boot_sec="5min", on_unit_inactive_sec="24h", persistent=False)
    out = timer_triggers(t)
    assert "OnBootSec=5min" in out
    assert "OnUnitInactiveSec=24h" in out
    assert "Persistent=false" in out


def test_timer_triggers_calendar() -> None:
    t = TimerConfig(on_calendar="*-*-* 04:30:00", persistent=True)
    out = timer_triggers(t)
    assert "OnCalendar=*-*-* 04:30:00" in out
    assert "Persistent=true" in out
    assert "OnBootSec" not in out
