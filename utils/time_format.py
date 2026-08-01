# utils/time_format.py
"""Central place for turning timestamps into human-friendly strings.
Keep all display-time formatting behind these two functions so the format
(and timezone) can be changed in one place later."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DISPLAY_TZ = ZoneInfo("America/New_York")
_DISPLAY_FORMAT = "%b %-d, %Y - %H:%M:%S"


def parse_timestamp(value: str | datetime | None) -> datetime | None:
    """Parse an ISO-8601 string (or pass through a datetime) into a tz-aware
    datetime normalized to UTC. Naive input is assumed to already be UTC;
    offset-aware input (e.g. SEC's RSS feed, which reports ET) is converted —
    every timestamp downstream (aggregator, DB, display) is stored/compared
    consistently in UTC regardless of the source's original offset."""
    if value is None:
        return None
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_display(value: str | datetime | None) -> str:
    """Human-friendly rendering in DISPLAY_TZ, e.g. 'Jan 5, 2026 - 14:23:01'."""
    dt = parse_timestamp(value)
    if dt is None:
        return "N/A"
    return dt.astimezone(DISPLAY_TZ).strftime(_DISPLAY_FORMAT)
