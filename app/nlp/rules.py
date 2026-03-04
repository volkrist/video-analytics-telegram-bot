"""
Детерминированный парсер интентов без LLM (fallback для тестовых формулировок).
"""
from __future__ import annotations

import re
from typing import Any

# месяц -> номер
RU_MONTH = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _detect_metric(text: str) -> str:
    t = text.lower()
    if "лайк" in t:
        return "likes"
    if "коммент" in t:
        return "comments"
    if "жалоб" in t:
        return "reports"
    return "views"


def _parse_date_ru(day: int, month: int, year: int) -> str:
    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_year(text: str) -> int:
    m = re.search(r"20\d{2}", text)
    return int(m.group(0)) if m else 2025


def _parse_single_date_ru(text: str) -> str | None:
    """Извлечь одну дату вида '28 ноября 2025' или '1 ноября 2025'."""
    text = text.strip().lower()
    for month_name, month_num in RU_MONTH.items():
        if month_name not in text:
            continue
        # "28 ноября 2025" или "1 ноября 2025"
        pat = r"(\d{1,2})\s*" + re.escape(month_name) + r"\s*(\d{4})?"
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else _extract_year(text)
            return _parse_date_ru(day, month_num, year)
    return None


def _parse_date_range_ru(text: str) -> tuple[str | None, str | None]:
    """
    Извлечь диапазон: "с 1 ноября 2025 по 5 ноября 2025" или "с 1 по 5 ноября 2025".
    Возвращает (date_from, date_to) в YYYY-MM-DD.
    """
    text_lower = text.lower().strip()
    year = _extract_year(text)

    # "с 1 по 5 ноября 2025" — один месяц
    month_pattern = "|".join(re.escape(k) for k in RU_MONTH)
    m = re.search(r"с\s*(\d{1,2})\s*по\s*(\d{1,2})\s*(\d{4})?\s*(" + month_pattern + r")", text_lower)
    if m:
        d1, d2 = int(m.group(1)), int(m.group(2))
        month_name = m.group(4)
        month_num = RU_MONTH[month_name]
        y = int(m.group(3)) if m.group(3) else year
        return _parse_date_ru(d1, month_num, y), _parse_date_ru(d2, month_num, y)

    # "с 1 ноября 2025 по 5 ноября 2025"
    parts = re.split(r"\s+по\s+", text_lower, maxsplit=1)
    if len(parts) == 2:
        from_part = re.sub(r"^с\s+", "", parts[0]).strip()
        to_part = parts[1].strip()
        date_from = _parse_single_date_ru(from_part + " " + str(year))
        date_to = _parse_single_date_ru(to_part + " " + str(year))
        if date_from and date_to:
            return date_from, date_to

    return None, None


def try_parse_rules(text: str) -> dict[str, Any] | None:
    """
    Распознать интент по правилам (регулярки/ключевые фразы).
    Возвращает dict в формате LLM или None.
    """
    if not text or not text.strip():
        return None
    t = text.strip().lower()

    # 1) Сколько всего видео (в системе)?
    if re.search(r"сколько\s+всего\s+видео", t) or re.search(r"всего\s+видео\s+в\s+системе", t):
        return {"intent": "count_videos_total"}

    # 2) Сколько видео у креатора с id N вышло с ... по ... включительно
    m_creator = re.search(r"креатора\s+с\s*id\s*(\d+)", t, re.I)
    if m_creator:
        creator_id = int(m_creator.group(1))
        date_from, date_to = _parse_date_range_ru(text)
        if date_from and date_to:
            return {
                "intent": "count_videos_by_creator_date_range",
                "creator_id": creator_id,
                "date_from": date_from,
                "date_to": date_to,
            }

    # 3) Сколько видео набрало больше N просмотров (в т.ч. "100 000" или "100000")
    m_alt = re.search(r"набрало\s+больше\s+([\d\s]+)\s*просмотр", t)
    if m_alt:
        try:
            threshold = int(re.sub(r"\s+", "", m_alt.group(1)))
            return {"intent": "count_videos_views_gt", "threshold": threshold}
        except ValueError:
            pass
    m_views = re.search(r"больше\s+([\d\s]+)\s*просмотр", t)
    if m_views:
        try:
            threshold = int(re.sub(r"\s+", "", m_views.group(1)))
            return {"intent": "count_videos_views_gt", "threshold": threshold}
        except ValueError:
            pass

    # 4) На сколько просмотров в сумме выросли все видео DATE
    metric = _detect_metric(text)
    m_delta = re.search(r"на\s+сколько\s+.+выросл|прирост|в\s+сумме\s+выросл", t)
    if m_delta:
        single = _parse_single_date_ru(text)
        if single:
            return {
                "intent": "sum_delta_on_date",
                "metric": metric,
                "date": single,
            }

    # 5) Сколько разных видео получали новые просмотры DATE
    m_distinct = re.search(r"сколько\s+разных\s+видео\s+.+новые?\s+(просмотр|лайк|коммент|жалоб)", t)
    if not m_distinct:
        m_distinct = re.search(r"разных\s+видео\s+.+получал", t)
    if m_distinct:
        single = _parse_single_date_ru(text)
        if single:
            return {
                "intent": "count_distinct_videos_with_positive_delta_on_date",
                "metric": metric,
                "date": single,
            }

    # Fallback для даты в формате "28 ноября 2025" при фразах про прирост/выросли
    if re.search(r"выросл|прирост", t):
        single = _parse_single_date_ru(text)
        if single:
            return {"intent": "sum_delta_on_date", "metric": metric, "date": single}
    if re.search(r"получал.новые?\s+(просмотр|лайк)|новые?\s+просмотр", t):
        single = _parse_single_date_ru(text)
        if single:
            return {
                "intent": "count_distinct_videos_with_positive_delta_on_date",
                "metric": metric,
                "date": single,
            }

    return None
