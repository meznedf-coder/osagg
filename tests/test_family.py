"""EXPLAIN says which indices a table reads and warns about the indices of the same family it does not read (0.2.11):
an alias put on indices with a wildcard keeps the indices of that moment, so the index created on the 1st of the
next month is not read (the production report of 2 October: Discover read 215 documents, the alias none of them)."""

from __future__ import annotations

from types import SimpleNamespace

from osagg.dbapi import _family_others, _family_pattern, _indices_read, _norm_name

ALIAS = "script_jobs"
SEPT = "script-jobs-2026.09"
OCT = "script-jobs-2026.10"


class Fake:
    kind = "direct"

    def __init__(self, members):
        self.members = list(members)          # the indices the alias points to

    def list_tables(self, patterns=None):
        pattern = patterns[0]
        if pattern == "script_jobs*":          # the alias, as _resolve/index answers it
            return [(ALIAS, "alias")]
        assert pattern == "script*jobs*", pattern
        return [(ALIAS, "alias"), (SEPT, "index"), (OCT, "index"), (".hidden-script-jobs", "index")]

    def count(self, index, query):
        return {"script_jobs*": 120, OCT: 215}.get(index, 0)


def conn_with(members):
    meta = SimpleNamespace(indices=list(members))
    return SimpleNamespace(transport=Fake(members), table_meta=lambda name: meta)


def test_an_index_the_alias_does_not_read_is_named():
    lines = _indices_read(conn_with([SEPT]), "script_jobs*")
    assert lines[0] == "-- script_jobs* reads 1 index: script-jobs-2026.09, 120 documents"
    warning = lines[1]
    assert warning.startswith('-- WARNING: 1 index of the same name (apart from "-", "_" and ".") not read by '
                              "script_jobs*: script-jobs-2026.10, 215 documents.")
    assert "put the alias in the index template" in warning
    assert warning.endswith('To read every index of the family: FROM "script-jobs*"')


def test_nothing_is_said_when_every_index_is_read():
    assert len(_indices_read(conn_with([SEPT, OCT]), "script_jobs*")) == 1


def test_the_family():
    assert _norm_name("Acme_RISK-batch.2026") == "acme-risk-batch-2026"
    assert _family_pattern("acme_risk_batch_jobs*", "acme-risk_batch-jobs-2026.10") == \
        "acme-risk_batch-jobs*"
    assert _family_pattern("batch_jobs-*", "batch-jobs-2026.10") == "batch-jobs-*"
    t = Fake([SEPT])
    assert _family_others(t, "script_jobs*", {SEPT}) == [OCT]          # hidden indices and the alias left out
    assert _family_others(t, "scriptjobs", set()) == []                  # no separator: no family to look for
    t.kind = "trino"
    assert _family_others(t, "script_jobs*", {SEPT}) == []               # the direct transport only
