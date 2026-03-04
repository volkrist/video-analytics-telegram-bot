"""
Общие хелперы для парсинга даты/времени из текста и для передачи в БД (всегда datetime UTC).
"""
from __future__ import annotations

from datetime import datetime, time, timezone


def parse_time(s: str) -> time | None:
    """Парсинг времени: '10:00' или '10:00:00' → time(10, 0, 0)."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def parse_date_iso(s: str) -> datetime | None:
    """YYYY-MM-DD → datetime 00:00:00 UTC."""
    s = (s or "").strip()[:10]
    if len(s) != 10:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def datetime_from_date_and_time(date_str: str, time_str: str) -> datetime | None:
    """Собрать datetime UTC из даты YYYY-MM-DD и времени HH:MM или HH:MM:SS."""
    base = parse_date_iso(date_str)
    t = parse_time(time_str)
    if base is None or t is None:
        return None
    return base.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
