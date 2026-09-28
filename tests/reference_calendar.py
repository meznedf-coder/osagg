"""Reference implementation of the business-date labels, written independently of
osagg.calendar: the tests check that osagg's built-in POSITION_LABEL column gives the same
labels, the same inverse (label -> dates) and the same D-1 anchor.

Labels, relative to the *session date* (weekdays before 14:00 belong to the previous
business day, weekends roll back to Friday):
  D-x  the x-th previous business day        W-x  same weekday x weeks before the session date
  Y-x  (osagg extension, label_years=true) same weekday 52*x weeks before the session date
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Union


def _get_effective_dates(now: Union[datetime, date, None] = None) -> tuple[date, date]:
    """Helper to compute the base calendar date and the effective session date."""
    if now is None:
        now = datetime.now()
    if not isinstance(now, datetime):
        now = datetime.combine(now, time(14, 0))  # Default to post-cutoff if only date is passed

    today = now.date()

    # Calculate effective session date (for W-x logic)
    if now.weekday() >= 5:  # Saturday (5) or Sunday (6) -> Roll back to Friday
        session_date = today - timedelta(days=now.weekday() - 4)
    elif now.time() < time(14, 0):  # Weekday before 14:00 -> Roll back to previous business day
        session_date = today - timedelta(days=3 if today.weekday() == 0 else 1)
    else:  # Weekday after 14:00
        session_date = today

    return today, session_date


def convert_date_or_label(input_val: str, now: Union[datetime, date, None] = None) -> Union[str, int]:
    """Converts an 8-digit date string to a label, or a label to an integer days delta."""
    today, session_date = _get_effective_dates(now)

    # Case 1: Convert Date String -> Label
    if input_val.isdigit() and len(input_val) == 8:
        target_date = datetime.strptime(input_val, "%Y%m%d").date()
        delta_w = (session_date - target_date).days

        if delta_w > 0 and delta_w % 7 == 0:
            return f"W-{delta_w // 7}"

        days_ago = (today - target_date).days
        d_count = sum(1 for d in range(1, days_ago + 1) if (today - timedelta(days=d)).weekday() < 5)
        return f"D-{d_count}" if d_count > 0 else "D"

    # Case 2: Convert Label -> Integer Days Delta
    input_upper = input_val.upper()
    if input_upper == 'D':
        return 0

    if '-' in input_upper:
        label, num_str = input_upper.split('-')
        num = int(num_str)
        if label == 'W':
            target_date = session_date - timedelta(days=7 * num)
            return (today - target_date).days
        elif label == 'D':
            count = delta = 0
            while count < num:
                delta += 1
                if (today - timedelta(days=delta)).weekday() < 5:
                    count += 1
            return delta

    raise ValueError("Invalid input format. Use an 8-digit date string or a label ('W-x' / 'D-x').")


def convert_date_or_label_y(input_val: str, now: Union[datetime, date, None] = None) -> Union[str, int]:
    """convert_date_or_label with the Y-x rule checked first (W-52 reads Y-1)."""
    today, session_date = _get_effective_dates(now)
    if input_val.isdigit() and len(input_val) == 8:
        target_date = datetime.strptime(input_val, "%Y%m%d").date()
        delta = (session_date - target_date).days
        if delta > 0 and delta % 364 == 0:
            return f"Y-{delta // 364}"
    elif input_val.upper().startswith("Y-"):
        num = int(input_val.split("-", 1)[1])
        return (today - (session_date - timedelta(days=364 * num))).days
    return convert_date_or_label(input_val, now)
