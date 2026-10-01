"""Built-in business-date label column: POSITION_LABEL, computed from POSITION_DATE, checked
against a reference implementation written independently (reference_calendar.py): labels,
their inverse (label -> dates), the D-1 anchor, the 14:00 cut-off and weekends, and filters on
the label pushed down as POSITION_DATE filters without any request. Offline: no OpenSearch."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest
import sqlglot

import osagg
import reference_calendar as ref
from osagg import calendar, dbapi, duck
from osagg.metadata import ID_FIELD, Field, TableMeta, with_label_column
from osagg.planner import Planner, Settings

INDEX = "labels"
T = f'"default"."{INDEX}"'
NOT_DATES = ["N/A", "", "2026-09-25", "20261399", "abc", "99999999", "2026092", "d-1"]


def ref_label(value: str | None, now: dt.datetime, years: bool) -> str | None:
    """Reference function; values that are not dates have no label."""
    if value is None or not (value.isdigit() and len(value) == 8):
        return None
    try:
        return (ref.convert_date_or_label_y if years else ref.convert_date_or_label)(value, now)
    except ValueError:
        return None


def ref_anchor(now: dt.datetime) -> dt.date:
    """The D-1 position date with the reference function (label -> days back)."""
    return now.date() - dt.timedelta(days=ref.convert_date_or_label("D-1", now))


def ref_shift(value: str | None, now: dt.datetime) -> int | None:
    if value is None or not (value.isdigit() and len(value) == 8):
        return None
    try:
        return (ref_anchor(now) - dt.datetime.strptime(value, "%Y%m%d").date()).days
    except ValueError:
        return None


NOWS = {
    "fri_after_cutoff": dt.datetime(2026, 9, 25, 15, 0),
    "fri_1359": dt.datetime(2026, 9, 25, 13, 59),
    "fri_1400": dt.datetime(2026, 9, 25, 14, 0),
    "mon_morning": dt.datetime(2026, 9, 28, 10, 0),
    "saturday": dt.datetime(2026, 9, 26, 11, 0),
    "sunday": dt.datetime(2026, 9, 27, 18, 0),
    "tue_late_december": dt.datetime(2026, 12, 29, 16, 0),
}


def _moments():
    start = dt.datetime(2026, 9, 21)
    for day in range(14):
        for hm in ((9, 30), (13, 59), (14, 0), (23, 59)):
            yield start + dt.timedelta(days=day, hours=hm[0], minutes=hm[1])


def test_labels_match_the_reference_functions():
    n = 0
    for now in _moments():
        for years in (True, False):
            key = calendar.asof_key(now, years=years)
            for back in range(-20, 420):
                d = (now.date() - dt.timedelta(days=back)).strftime("%Y%m%d")
                # the reference (the production function) calls every later date "D"; osagg (0.2.10) keeps
                # "D" for today's position date and calls the later ones D+1, D+2...
                want = ref_label(d, now, years) if back >= 0 else f"D+{-back}"
                assert calendar.label(d, key) == want, (now, d, years)
                n += 1
    assert n > 40_000
    for v in NOT_DATES + [None]:
        assert calendar.label(v, calendar.asof_key(NOWS["fri_1400"])) is None


def test_dates_for_labels_is_the_exact_inverse():
    labels = ["D", "D-1", "D-2", "D-5", "D-23", "W-1", "W-3", "W-52", "W-53", "Y-1", "Y-2",
              "d-1", "D-0", "W-01", "X-1"]
    for now in _moments():
        key = calendar.asof_key(now)
        today = now.date()
        window = [(today - dt.timedelta(days=b)).strftime("%Y%m%d") for b in range(-7, 760)]
        for lbl in labels + ["D+1", "D+3", "W+1"]:
            expected = sorted(d for d in window if calendar.label(d, key) == lbl)
            assert calendar.dates_for_labels([lbl], key) == expected, (now, lbl)
            assert all(ref_label(d, now, True) == lbl for d in expected if not lbl.startswith("D+"))
        # "D" is today's position date: one date from Tuesday to Friday (on Monday the weekend before it too,
        # which no weekday precedes), never the dates after today, and nothing is open-ended any more
        d_dates = calendar.dates_for_labels(["D"], key)
        assert today.strftime("%Y%m%d") in d_dates and all(x <= today.strftime("%Y%m%d") for x in d_dates)
        if today.weekday() in (1, 2, 3, 4):
            assert d_dates == [today.strftime("%Y%m%d")]
        assert calendar.dates_for_labels(["D+1"], key) == [(today + dt.timedelta(days=1)).strftime("%Y%m%d")]
        assert calendar.open_ended_from(["D", "W-1"], key) is None


def test_cutoff_weekend_and_label_timezone():
    key = lambda **kw: dbapi.Connection(host="localhost", **kw).label_key()  # noqa: E731
    assert key(label_now="2026-09-25T13:59:59") == "20260925/20260924/Y"
    assert key(label_now="2026-09-25T14:00:00") == "20260925/20260925/Y"
    assert key(label_now="2026-09-28T09:00:00") == "20260928/20260925/Y"   # Monday -> Friday
    assert key(label_now="2026-09-26T16:00:00") == "20260926/20260925/Y"   # Saturday -> Friday
    assert key(label_now="2026-09-27T16:00:00", label_years="false") == "20260927/20260925/-"
    assert key(label_now="2026-09-25T15:00:00", label_cutoff="16:30") == "20260925/20260924/Y"
    # an instant is read on the label zone's wall clock: 12:30 UTC is 14:30 in Paris
    assert key(label_now="2026-09-25T12:30:00+00:00", label_timezone="Europe/Paris") \
        == "20260925/20260925/Y"
    assert key(label_now="2026-09-25T12:30:00+00:00", label_timezone="UTC") == "20260925/20260924/Y"
    # default: the Superset process clock
    now = dt.datetime.now()
    assert key() in {calendar.asof_key(now), calendar.asof_key(dt.datetime.now())}
    with pytest.raises(osagg.InterfaceError):
        dbapi.Connection(host="localhost", label_timezone="Mars/Olympus")


def test_planner_pushes_labels_without_any_request():
    base = TableMeta(INDEX, [INDEX], {
        "POSITION_DATE": Field("POSITION_DATE", "keyword", "VARCHAR", "POSITION_DATE", "POSITION_DATE"),
        "AMOUNT": Field("AMOUNT", "long", "BIGINT", "AMOUNT", "AMOUNT"), "_id": ID_FIELD})
    meta = with_label_column(base, "POSITION_DATE", "POSITION_LABEL")
    assert meta.fields["POSITION_LABEL"].virtual == "label:POSITION_DATE"
    assert with_label_column(meta, "POSITION_DATE", "position_label") is meta   # already there
    assert with_label_column(base, "AMOUNT", "X") is base                      # not a keyword date
    con = duck.locked_session("Europe/Paris")
    key = calendar.asof_key(dt.datetime(2026, 9, 25, 10, 0))                   # session 20260924
    planner = Planner(lambda n: meta if n == INDEX else None,
                      Settings(tz=ZoneInfo("Europe/Paris"), label_key=key),
                      lambda node: con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0])
    plan = planner.plan(sqlglot.parse_one(
        f"SELECT SUM(\"AMOUNT\") FROM {T} WHERE \"POSITION_LABEL\" IN ('D-1', 'W-1', 'Y-1')", read="duckdb"))
    scan = plan.scans[0]
    assert scan.mode == "global"
    assert scan.query == {"terms": {"POSITION_DATE": ["20250925", "20260917", "20260924"]}}
    plan = planner.plan(sqlglot.parse_one(f'SELECT * FROM {T} LIMIT 5', read="duckdb"))
    assert [f.name for f in plan.scans[0].fields] == ["POSITION_DATE", "AMOUNT", "_id"]
    assert plan.residual.expressions[-1].alias == "POSITION_LABEL"
    con.close()


def test_shift_days_matches_the_reference_anchor():
    for now in _moments():
        key = calendar.asof_key(now)
        assert calendar.anchor(key) == ref_anchor(now), now
        for back in range(-10, 400, 7):
            d = (now.date() - dt.timedelta(days=back)).strftime("%Y%m%d")
            assert calendar.shift_days(d, key) == ref_shift(d, now)
    assert calendar.shift_days("N/A", calendar.asof_key(NOWS["fri_1400"])) is None


def test_planner_pushes_aligned_time_ranges_without_any_request():
    base = TableMeta(INDEX, [INDEX], {
        "@timestamp_date": Field("@timestamp_date", "date", "TIMESTAMP", "@timestamp_date", "@timestamp_date"),
        "POSITION_DATE": Field("POSITION_DATE", "keyword", "VARCHAR", "POSITION_DATE", "POSITION_DATE"),
        "_id": ID_FIELD})
    meta = with_label_column(base, "POSITION_DATE", "POSITION_LABEL", "@timestamp_date", "POSITION_TIME")
    con = duck.locked_session("Europe/Paris")
    key = calendar.asof_key(dt.datetime(2026, 9, 25, 10, 0))                   # D-1 = 20260924
    planner = Planner(lambda n: meta if n == INDEX else None,
                      Settings(tz=ZoneInfo("Europe/Paris"), label_key=key),
                      lambda node: con.execute("SELECT " + node.sql(dialect="duckdb")).fetchone()[0])
    plan = planner.plan(sqlglot.parse_one(
        f"SELECT TIME_BUCKET(INTERVAL '10 minutes', \"POSITION_TIME\") AS t, \"POSITION_LABEL\", COUNT(*) "
        f"FROM {T} WHERE \"POSITION_TIME\" >= TIMESTAMP '2026-09-24 14:00:00' AND "
        f"\"POSITION_LABEL\" IN ('D-1', 'W-1') GROUP BY 1, 2", read="duckdb"))
    scan = plan.scans[0]
    assert scan.mode == "two-level"
    kinds = sorted(k.kind for _, k in scan.keys)
    assert kinds == ["date_histogram", "terms"]
    q = str(scan.query)
    # W-1 (20260917) is read from 2026-09-17 14:00 Paris = 12:00 UTC
    assert "'20260917'" in q and "'2026-09-17T14:00:00.000+02:00'" in q
    con.close()
