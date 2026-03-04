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


def _parse_month_year_ru(text: str) -> tuple[str | None, str | None]:
    """Извлечь месяц+год: «в июне 2025» → (date_from, date_to_exclusive), например ('2025-06-01', '2025-07-01')."""
    t = text.lower()
    y = _extract_year(text)
    for mname, mnum in RU_MONTH.items():
        if re.search(rf"\b{re.escape(mname)}\b", t):
            to_y, to_m = (y + 1, 1) if mnum == 12 else (y, mnum + 1)
            return f"{y:04d}-{mnum:02d}-01", f"{to_y:04d}-{to_m:02d}-01"
        # «июне», «июнь» — основа «июн» от «июня» с границей слова
        if mname.endswith("я") and re.search(rf"\b{re.escape(mname[:-1])}\w*\b", t):
            to_y, to_m = (y + 1, 1) if mnum == 12 else (y, mnum + 1)
            return f"{y:04d}-{mnum:02d}-01", f"{to_y:04d}-{to_m:02d}-01"
    return None, None


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
    Возвращает (date_from, date_to) в YYYY-MM-DD. date_to всегда включительно («по 5» = 5-й день входит).
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

    # "с 1 ноября 2025 по 5 ноября 2025" или "в период с 1 ноября 2025 по 5 ноября 2025 включительно"
    # Сначала вырезаем подстроку с периодом, чтобы не путать с "креатор с id"
    period_match = re.search(
        r"(в\s+период\s+)?с\s+(\d{1,2}\s*" + month_pattern + r"\s*\d{4})\s+по\s+(\d{1,2}\s*" + month_pattern + r"\s*\d{4})",
        text_lower,
    )
    if period_match:
        from_part = period_match.group(2).strip()  # "1 ноября 2025"
        to_part = period_match.group(3).strip()   # "5 ноября 2025"
        date_from = _parse_single_date_ru(from_part + " " + str(year))
        date_to = _parse_single_date_ru(to_part + " " + str(year))
        if date_from and date_to:
            return date_from, date_to

    parts = re.split(r"\s+по\s+", text_lower, maxsplit=1)
    if len(parts) == 2:
        # Убрать всё до "в период с " или "с " (дата может быть в середине фразы)
        from_part = re.sub(r"^.*?(в\s+период\s+)?с\s+", "", parts[0]).strip()
        to_part = parts[1].strip()
        to_part = re.sub(r"\s+включительно.*$", "", to_part, flags=re.I)
        to_part = re.sub(r"\?.*$", "", to_part).strip()
        date_from = _parse_single_date_ru(from_part + " " + str(year))
        date_to = _parse_single_date_ru(to_part + " " + str(year))
        if date_from and date_to:
            return date_from, date_to

    return None, None


def _try_parse_rules_emergency(text: str, t: str) -> dict[str, Any] | None:
    """
    Emergency: сумма прироста в временном интервале (time-range + delta).
    Признаки: время (HH:MM или «с … до …»), выросли/прирост/изменения/замеры, дата словами, опционально креатор с id.
    Дата парсится словами (28 ноября 2025) через _parse_single_date_ru.
    """
    if not re.search(r"выросл|прирост|изменени|между\s+замерами|замер|просмотр", t):
        return None
    date_str = _parse_single_date_ru(text)
    if not date_str:
        return None
    time_from = time_to = None
    m = re.search(r"с\s*(\d{1,2})(?::(\d{2}))?\s*до\s*(\d{1,2})(?::(\d{2}))?", t)
    if m:
        h1, m1, h2, m2 = m.group(1), m.group(2), m.group(3), m.group(4)
        time_from = f"{int(h1):02d}:{int(m1 or 0):02d}"
        time_to = f"{int(h2):02d}:{int(m2 or 0):02d}"
    else:
        times = re.findall(r"\d{1,2}:\d{2}", t)
        if len(times) >= 2:
            time_from, time_to = times[0], times[1]
    if not time_from or not time_to:
        m_bare = re.search(r"с\s*(\d{1,2})\b.*?до\s*(\d{1,2})\b", t)
        if m_bare:
            time_from = f"{int(m_bare.group(1)):02d}:00"
            time_to = f"{int(m_bare.group(2)):02d}:00"
    if not time_from or not time_to:
        return None
    metric = "views"
    creator_id = None
    mid = re.search(r"\s+id\s+([a-f0-9\-]+)", t, re.I)
    if mid:
        creator_id = mid.group(1).strip()
    if creator_id:
        return {
            "intent": "sum_delta_in_time_range_by_creator",
            "metric": metric,
            "creator_id": creator_id,
            "date": date_str,
            "time_from": time_from,
            "time_to": time_to,
        }
    return {
        "intent": "sum_delta_in_time_range",
        "metric": metric,
        "date": date_str,
        "time_from": time_from,
        "time_to": time_to,
    }


def try_parse_rules(text: str, emergency: bool = False) -> dict[str, Any] | None:
    """
    Распознать интент по правилам (регулярки/ключевые фразы).
    emergency=True: только emergency-правило (time-range + delta), когда LLM недоступен.
    """
    if not text or not text.strip():
        return None
    t = text.strip().lower()
    if emergency:
        return _try_parse_rules_emergency(text, t)

    # 1) Сколько всего видео (в системе)?
    if re.search(r"сколько\s+всего\s+видео", t) or re.search(r"всего\s+видео\s+в\s+системе", t):
        return {"intent": "count_videos_total"}

    # 1a) SUM финальной метрики по опубликованным видео за месяц: «суммарное количество просмотров … в июне 2025»
    if re.search(r"суммарн|сумма|итого", t) and re.search(r"опубликован|публик|выложен|вышедш", t):
        metric = _detect_metric(text)
        date_from, date_to_excl = _parse_month_year_ru(text)
        if date_from and date_to_excl:
            return {
                "intent": "sum_final_metric_in_period",
                "metric": metric,
                "date_from": date_from,
                "date_to_exclusive": date_to_excl,
            }

    # 1b) Замеры/снапшоты, где прирост за час отрицательный (delta < 0)
    if re.search(r"замер|снапшот|почасов", t) and re.search(r"отрицательн|меньше\s*0|ниже\s*0", t):
        metric = "views"
        if re.search(r"лайк", t):
            metric = "likes"
        elif re.search(r"коммент", t):
            metric = "comments"
        elif re.search(r"жалоб", t):
            metric = "reports"
        return {
            "intent": "count_snapshots_delta_cmp",
            "metric": metric,
            "op": "lt",
            "value": 0,
        }

    _creator = r"(креатор(а)?|creator|автор|создатель)"
    # 2a) У креатора с id X … больше N … по итоговой статистике (таблица videos)
    if re.search(r"итоговой\s+статистике|итоговой\s+статистик", t):
        m_creator_final = re.search(
            _creator + r"\s+с\s*id\s*([^\s,;]+).*?больше\s+([\d\s]+)\s*(просмотр|лайк|коммент|жалоб)",
            t,
            re.I | re.DOTALL,
        )
        if m_creator_final:
            try:
                creator_id = m_creator_final.group(2).strip()
                threshold = int(re.sub(r"\s+", "", m_creator_final.group(3)))
                metric = _detect_metric(m_creator_final.group(4) or "")
                return {
                    "intent": "count_videos_by_creator_metric_gt_final",
                    "creator_id": creator_id,
                    "threshold": threshold,
                    "metric": metric,
                }
            except ValueError:
                pass

    # 2a') Креатора с id X набрали больше N просмотров (без слова «итоговой» — тоже итоговая статистика)
    m_creator_views = re.search(
        _creator + r"\s+с\s*id\s*([^\s,;]+).*?набрали\s+больше\s+([\d\s]+)\s*просмотр",
        t,
        re.I | re.DOTALL,
    )
    if m_creator_views:
        try:
            creator_id = m_creator_views.group(2).strip()
            threshold = int(re.sub(r"\s+", "", m_creator_views.group(3)))
            return {
                "intent": "count_videos_by_creator_metric_gt_final",
                "creator_id": creator_id,
                "threshold": threshold,
                "metric": "views",
            }
        except ValueError:
            pass

    # 2a'') Запас: "по итоговой статистике" + "с id" + "больше N просмотр" — точно запрос по креатору
    if re.search(r"итоговой\s+статистике|итоговой\s+статистик", t) and re.search(r"\s+с\s+id\s+", t, re.I) and "больше" in t and "просмотр" in t:
        mid = re.search(r"\s+с\s+id\s+([^\s,;?]+)", t, re.I)
        mth = re.search(r"больше\s+([\d\s]+)\s*просмотр", t)
        if mid and mth:
            try:
                cid = mid.group(1).strip()
                threshold = int(re.sub(r"\s+", "", mth.group(1)))
                if cid and threshold is not None:
                    return {
                        "intent": "count_videos_by_creator_metric_gt_final",
                        "creator_id": cid,
                        "threshold": threshold,
                        "metric": "views",
                    }
            except ValueError:
                pass

    # 2b) Видео по дате публикации: (опубликовал|вышло|...) креатор с id N … в период с … по …
    # Именованные группы, чтобы не зависеть от номера group() при изменении regex.
    date_range_verbs = r"(опубликовал|опубликовали|опубликован|опубликовано|выложил|выложены|вышло|вышли)"
    m_date_range = re.search(
        rf"{date_range_verbs}.*?{_creator}\s+с\s*id\s*(?P<cid>[^\s,;]+).*?"
        r"(в\s+период\s+)?с\s+(?P<from>.+?)\s+по\s+(?P<to>.+?)(\s+включительно)?(\?|\.|$)",
        t,
        re.I | re.DOTALL,
    )
    if m_date_range:
        cid = (m_date_range.group("cid") or "").strip()
        if not cid:
            pass  # fall through to 2b''
        else:
            date_from_raw = (m_date_range.group("from") or "").strip()
            date_to_raw = (m_date_range.group("to") or "").strip()
            # Обрезать хвост у второй даты: "5 ноября 2025 включительно? Под периодом …"
            date_to_raw = re.sub(r"\s+включительно.*$", "", date_to_raw, flags=re.I)
            date_to_raw = re.sub(r"\?.*$", "", date_to_raw).strip()
            year = _extract_year(text)
            date_from = _parse_single_date_ru(date_from_raw + " " + str(year))
            date_to = _parse_single_date_ru(date_to_raw + " " + str(year))
            if date_from and date_to:
                return {
                    "intent": "count_videos_by_creator_date_range",
                    "creator_id": cid,
                    "date_from": date_from,
                    "date_to": date_to,
                }

    # 2b'') Запас: "опубликовал/период" + креатор с id + период с даты по дату
    if re.search(r"опубликовал|период", t) and re.search(_creator + r"\s+с\s*id\s+[^\s,;]+", t):
        date_from, date_to = _parse_date_range_ru(text)
        if date_from and date_to:
            mid = re.search(r"\s+с\s+id\s+(?P<cid>[^\s,;?]+)", t, re.I)
            if mid:
                cid = (mid.group("cid") or "").strip()
                if cid:
                    return {
                        "intent": "count_videos_by_creator_date_range",
                        "creator_id": cid,
                        "date_from": date_from,
                        "date_to": date_to,
                    }

    # 2b') Креатор/creator/автор с id N … с ... по ... (без глагола впереди)
    m_creator = re.search(_creator + r"\s+с\s*id\s*(?P<cid>[^\s,;]+)", t, re.I)
    if m_creator:
        creator_id = (m_creator.group("cid") or "").strip()
        if creator_id:
            date_from, date_to = _parse_date_range_ru(text)
            if date_from and date_to:
                return {
                    "intent": "count_videos_by_creator_date_range",
                    "creator_id": creator_id,
                    "date_from": date_from,
                    "date_to": date_to,
                }

    # 3) Сколько видео набрало больше/меньше/не меньше/не больше N (videos, итоговая статистика)
    # «за всё время» / «в системе» = всегда videos final, не snapshots
    _creator_with_id = re.search(_creator + r"\s+с\s*id\s*([^\s,;]+)", t)
    if _creator_with_id:
        cid = _creator_with_id.group(1).strip()
        metric = _detect_metric(text)
        for op_phrase, op in (("не\s+меньше", "gte"), ("не\s+больше", "lte"), ("меньше", "lt"), ("больше", "gt")):
            m_val = re.search(rf"{op_phrase}\s+([\d\s]+)\s*(просмотр|лайк|коммент|жалоб)", t)
            if m_val:
                try:
                    val = int(re.sub(r"\s+", "", m_val.group(1)))
                    return {
                        "intent": "count_videos_by_creator_metric_cmp_final",
                        "creator_id": cid,
                        "metric": metric,
                        "op": op,
                        "value": val,
                    }
                except ValueError:
                    pass
    if not re.search(_creator + r"\s+с\s*id\s*[^\s,;]+", t):
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
        metric = _detect_metric(text)
        for op_phrase, op in (("не\s+меньше", "gte"), ("не\s+больше", "lte"), ("меньше", "lt")):
            m_cmp = re.search(rf"(набрало\s+)?{op_phrase}\s+([\d\s]+)\s*(просмотр|лайк|коммент|жалоб)", t)
            if m_cmp:
                try:
                    val = int(re.sub(r"\s+", "", m_cmp.group(2)))
                    return {
                        "intent": "count_videos_metric_cmp_final",
                        "metric": metric,
                        "op": op,
                        "value": val,
                    }
                except ValueError:
                    pass

    # 4) На сколько просмотров в сумме выросли все видео DATE (не матчить, если в тексте время — отдать LLM)
    # Не возвращать snapshot-интенты, если фраза «за всё время»/«в системе» без маркеров прироста/замеров
    _has_time_in_text = bool(re.search(r"\d{1,2}:\d{2}", t) or re.search(r"с\s+.+до\s+", t))
    _skip_delta_rules = bool(re.search(r"за\s+вс[её]?\s+время|в\s+системе", t)) and not bool(
        re.search(r"прирост|замер|выросл|новые?\s+(просмотр|лайк|коммент|жалоб)|получал", t)
    )
    metric = _detect_metric(text)
    if not _has_time_in_text and not _skip_delta_rules:
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
    if not _has_time_in_text and not _skip_delta_rules:
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

    # Fallback для даты при фразах про прирост/выросли (не матчить, если в тексте время)
    if not _has_time_in_text and not _skip_delta_rules:
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
