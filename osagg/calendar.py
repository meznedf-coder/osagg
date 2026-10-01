"""Business-date labels of the position date (built into osagg).

Rules (same as the production `convert_date_or_label`):

* now -> (today, session date): on weekdays before the cut-off (14:00) the session
  date is the previous business day (Monday -> Friday); on Saturday / Sunday it is
  Friday; otherwise it is today.
* label of a position date (yyyymmdd), checked in this order:
    Y-x  the date is x*364 days (52 weeks, same weekday) before the session date
         (only when years are enabled)
    W-x  the date is x*7 days before the session date
    D-x  x = number of weekdays in [date, today - 1]   ("D" when 0)
    D+x  a position date after today, x calendar days later (osagg, 0.2.10: "D" is one date,
         today's; the production function also called the later dates "D")

A label as of a given moment is encoded as "<today>/<session>/<Y|->" (e.g.
"20260925/20260924/Y") so that one query uses one consistent calendar.
"""

from __future__ import annotations

import datetime as dt
import re
from functools import lru_cache

_LABEL = re.compile(r"^(D|W|Y)(?:([-+])(\d+))?$")


def effective_dates(now: dt.datetime, cutoff: dt.time = dt.time(14, 0)) -> tuple[dt.date, dt.date]:
    today = now.date()
    if now.weekday() >= 5:                      # Saturday / Sunday -> Friday
        session = today - dt.timedelta(days=now.weekday() - 4)
    elif now.time() < cutoff:                   # before the cut-off -> previous business day
        session = today - dt.timedelta(days=3 if today.weekday() == 0 else 1)
    else:
        session = today
    return today, session


def asof_key(now: dt.datetime, cutoff: dt.time = dt.time(14, 0), years: bool = True) -> str:
    today, session = effective_dates(now, cutoff)
    return f"{today:%Y%m%d}/{session:%Y%m%d}/{'Y' if years else '-'}"


def _parse_key(key: str) -> tuple[dt.date, dt.date, bool]:
    t, s, y = key.split("/")
    return (dt.datetime.strptime(t, "%Y%m%d").date(), dt.datetime.strptime(s, "%Y%m%d").date(),
            y == "Y")


def _weekdays_back(today: dt.date, target: dt.date) -> int:
    """Number of weekdays among today-1, today-2, ..., target (0 if target >= today)."""
    days_ago = (today - target).days
    if days_ago <= 0:
        return 0
    full_weeks, rest = divmod(days_ago, 7)
    count = full_weeks * 5
    for d in range(1, rest + 1):
        if (today - dt.timedelta(days=d)).weekday() < 5:
            count += 1
    return count


@lru_cache(maxsize=65536)
def label(date_str: str | None, key: str) -> str | None:
    """Label of a yyyymmdd position date (None if the value is not a date)."""
    if date_str is None or len(date_str) != 8 or not date_str.isdigit():
        return None
    try:
        target = dt.datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None
    today, session, years = _parse_key(key)
    if target > today:                          # a later position date: D+1, D+2... (D is today's only)
        return f"D+{(target - today).days}"
    delta = (session - target).days
    if years and delta > 0 and delta % 364 == 0:
        return f"Y-{delta // 364}"
    if delta > 0 and delta % 7 == 0:
        return f"W-{delta // 7}"
    n = _weekdays_back(today, target)
    return f"D-{n}" if n > 0 else "D"


def days_back(lbl: str, key: str) -> int | None:
    """Label -> number of calendar days before today (the production Case 2)."""
    m = _LABEL.match(lbl) if isinstance(lbl, str) else None
    if not m or (m.group(2) == "+" and m.group(1) != "D"):
        return None
    kind, num = m.group(1), int(m.group(3) or 0)
    today, session, _years = _parse_key(key)
    if kind == "D" and m.group(2) == "+":
        return -num                             # later than today
    if kind == "D":
        count = delta = 0
        while count < num:
            delta += 1
            if (today - dt.timedelta(days=delta)).weekday() < 5:
                count += 1
        return delta
    if kind == "W":
        return (today - (session - dt.timedelta(days=7 * num))).days
    return (today - (session - dt.timedelta(days=364 * num))).days


def dates_for_labels(labels: list[str], key: str) -> list[str]:
    """Every yyyymmdd date whose label is one of `labels` (exact: candidates are the inverse
    dates +/- a week, each checked with the forward rule; labels are compared as SQL does,
    case-sensitively). "D" is today's position date (and, on a Monday, the weekend before it,
    which no weekday precedes): one date on Tuesday to Friday."""
    wanted = {lbl for lbl in labels if isinstance(lbl, str)}
    today = _parse_key(key)[0]
    out: set[str] = set()
    for lbl in wanted:
        back = days_back(lbl, key)
        if back is None:
            continue
        center = today - dt.timedelta(days=back)
        for off in range(-7, 8):
            d = (center + dt.timedelta(days=off)).strftime("%Y%m%d")
            if label(d, key) in wanted:
                out.add(d)
    return sorted(out)


def open_ended_from(labels: list[str], key: str) -> str | None:
    """No label is open-ended since 0.2.10 (the dates after today are D+1, D+2...): None. Before, "D"
    held for every date from today on, so `POSITION_LABEL = 'D'` asked for today, the next 7 days
    and every later date."""
    return None


def anchor(key: str) -> dt.date:
    """The position date labelled D-1: the timeline every position date is moved onto."""
    today = _parse_key(key)[0]
    return today - dt.timedelta(days=days_back("D-1", key))


@lru_cache(maxsize=65536)
def shift_days(date_str: str | None, key: str) -> int | None:
    """Days to add to the execution time of a position date so that it lands on the D-1
    position date (W-1: +7, W-2: +14...); None if the value is not a date."""
    if date_str is None or len(date_str) != 8 or not date_str.isdigit():
        return None
    try:
        target = dt.datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None
    return (anchor(key) - target).days
