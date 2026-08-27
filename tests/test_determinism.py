"""Determinism contract for the SQL converter.

The converter must produce identical rows — and, on the pinned DuckDB, identical bytes —
for identical input, regardless of thread count. Historically it did not: every entity
and edge id used ``row_number() OVER ()`` with no ordering, site de-duplication used
unordered ``first()``, wide relationship arrays used unordered ``list()``, and two runs on
the same export produced different files (hashes in PR #26).

Fixture: ``tests/fixtures/isamples_export_fixture.parquet`` — 140 real export rows, all four
sources, selected deterministically (see tests/fixtures/README.md). Regenerate with::

    WITH a AS (SELECT *, row_number() OVER (PARTITION BY source_collection ORDER BY sample_identifier) rn
               FROM read_parquet(EXPORT) WHERE produced_by.sampling_site IS NOT NULL AND len(keywords) >= 2),
         b AS (SELECT *, row_number() OVER (PARTITION BY source_collection ORDER BY sample_identifier) rn
               FROM read_parquet(EXPORT) WHERE produced_by.sampling_site IS NOT NULL)
    SELECT * EXCLUDE rn FROM a WHERE rn <= 40 UNION SELECT * EXCLUDE rn FROM b WHERE rn <= 20
    ORDER BY sample_identifier
"""

import hashlib
from pathlib import Path

import duckdb
import pytest

import pqg.sql_converter as sc
from pqg.sql_converter import convert_isamples_sql

FIXTURE = Path(__file__).parent / "fixtures" / "isamples_export_fixture.parquet"
FORMATS = pytest.mark.parametrize("wide", [False, True], ids=["narrow", "wide"])

# The converter's site de-duplication key (sql_converter._convert_staged, dedupe_sites=True,
# site_precision=5). Kept in sync by test_site_dedupe_keeps_lowest_sample_identifier_member.
SITE_KEY = """'site:' || COALESCE(CAST(ROUND(produced_by.sampling_site.sample_location.latitude, 5) AS VARCHAR), 'NULL') || '_' ||
             COALESCE(CAST(ROUND(produced_by.sampling_site.sample_location.longitude, 5) AS VARCHAR), 'NULL') || '_' ||
             COALESCE(LOWER(TRIM(produced_by.sampling_site.label)), '') || '_' ||
             COALESCE(LOWER(TRIM(CAST(produced_by.sampling_site.place_name AS VARCHAR))), '')"""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _convert(out: Path, wide: bool, threads: int | None = None, src: Path = FIXTURE) -> str:
    """Run the converter, optionally pinning DuckDB's thread count, and return the sha256."""
    real_connect = duckdb.connect

    def connect(*a, **k):
        con = real_connect(*a, **k)
        if threads is not None:
            con.execute(f"SET threads={threads}")
        return con

    sc.duckdb.connect = connect
    try:
        convert_isamples_sql(str(src), str(out), wide=wide, verbose=False)
    finally:
        sc.duckdb.connect = real_connect
    return _sha256(out)


def test_fixture_exercises_every_ordering_path():
    """The fixture must contain the shapes whose ordering used to be arbitrary."""
    def q(where):
        return duckdb.sql(f"SELECT count(*) FROM read_parquet('{FIXTURE}') WHERE {where}").fetchone()[0]

    assert duckdb.sql(f"SELECT count(DISTINCT source_collection) FROM read_parquet('{FIXTURE}')").fetchone()[0] == 4
    assert q("len(keywords) >= 2") >= 100
    assert q("len(has_material_category) >= 1") >= 100
    assert q("len(related_resource) > 0") >= 20
    assert q("curation IS NOT NULL") >= 20
    assert q("len(produced_by.responsibility) > 0") >= 20
    # duplicate site KEYS (the converter's real key, not just the label) → site
    # de-duplication has to choose a winner
    dup_keys, dup_rows = duckdb.sql(f"""
        SELECT count(*), sum(c) FROM (SELECT {SITE_KEY} AS k, count(*) AS c
                FROM read_parquet('{FIXTURE}') WHERE produced_by.sampling_site IS NOT NULL
                GROUP BY 1 HAVING c > 1)""").fetchone()
    assert dup_keys >= 3, f"fixture has only {dup_keys} duplicated site keys"
    assert dup_rows >= 2 * dup_keys


@FORMATS
def test_two_runs_are_byte_identical(tmp_path, wide):
    assert _convert(tmp_path / "a.parquet", wide) == _convert(tmp_path / "b.parquet", wide)


@FORMATS
def test_thread_count_does_not_change_output(tmp_path, wide):
    """1 thread vs 16 threads: identical bytes, both formats."""
    assert _convert(tmp_path / "t1.parquet", wide, threads=1) == _convert(
        tmp_path / "t16.parquet", wide, threads=16
    )


@FORMATS
def test_row_ids_are_unique(tmp_path, wide):
    """Regression for the narrow offset bug (edges numbered from agent_max collided with
    curation / curation-agent / sample-relation ids) and the edge_sl_max fallback."""
    out = tmp_path / "out.parquet"
    _convert(out, wide)
    n, d = duckdb.sql(f"SELECT count(*), count(DISTINCT row_id) FROM read_parquet('{out}')").fetchone()
    assert n == d, f"{n - d} duplicate row_ids"


def test_edge_ids_do_not_collide_when_edge_tables_are_empty(tmp_path):
    """Input with no site coordinates → no GeospatialCoordLocation entities → the
    event_location and site_location edge tables are both empty, so three consecutive
    offsets fall through their COALESCE defaults. Ids must still chain without collision.
    (Codex finding on #26: edge_sl_max fell back to edge_ss_max instead of edge_el_max.
    With this data model event_location edges imply site_location edges, so the
    el-present/sl-absent case is unreachable; the empty-empty case is what can happen.)"""
    src = tmp_path / "no_coords.parquet"
    duckdb.sql(f"""
        COPY (SELECT * REPLACE (struct_update(produced_by, sampling_site :=
                   struct_update(produced_by.sampling_site, sample_location :=
                       struct_update(produced_by.sampling_site.sample_location,
                                     latitude := NULL::DOUBLE, longitude := NULL::DOUBLE))) AS produced_by)
              FROM read_parquet('{FIXTURE}'))
        TO '{src}' (FORMAT PARQUET)
    """)
    out = tmp_path / "out.parquet"
    _convert(out, wide=False, src=src)
    n, d, el, sl = duckdb.sql(f"""
        SELECT count(*), count(DISTINCT row_id),
               count(*) FILTER (WHERE p = 'sample_location'), count(*) FILTER (WHERE p = 'site_location')
        FROM read_parquet('{out}')""").fetchone()
    assert (el, sl) == (0, 0)
    assert n == d, f"{n - d} duplicate row_ids with empty location edge tables"


def test_duplicate_sample_identifier_is_rejected(tmp_path):
    src = tmp_path / "dup.parquet"
    duckdb.sql(f"COPY (SELECT * FROM read_parquet('{FIXTURE}') UNION ALL SELECT * FROM read_parquet('{FIXTURE}') LIMIT 141) TO '{src}' (FORMAT PARQUET)")
    with pytest.raises(ValueError, match="unique"):
        convert_isamples_sql(str(src), str(tmp_path / "out.parquet"), wide=True, verbose=False)


def test_site_dedupe_keeps_lowest_sample_identifier_member(tmp_path):
    """The documented winner rule: a de-duplicated site carries the fields of the member
    with the lowest sample_identifier."""
    out = tmp_path / "out.parquet"
    _convert(out, wide=True)
    bad = duckdb.sql(f"""
        WITH rule AS (
            SELECT {SITE_KEY} AS pid,
                   first(produced_by.sampling_site.description ORDER BY sample_identifier) AS d0,
                   first(source_collection ORDER BY sample_identifier) AS n0
            FROM read_parquet('{FIXTURE}') WHERE produced_by.sampling_site IS NOT NULL GROUP BY 1)
        SELECT count(*) FROM read_parquet('{out}') s JOIN rule USING (pid)
        WHERE s.otype = 'SamplingSite' AND (s.description IS DISTINCT FROM rule.d0 OR s.n IS DISTINCT FROM rule.n0)
    """).fetchone()[0]
    assert bad == 0


def test_no_unordered_window_functions_in_source():
    """Cheap guard (syntax, not semantics): every window function must declare an ORDER BY."""
    src = Path(sc.__file__).read_text()
    assert "OVER ()" not in src, "unordered window function reintroduced"
    assert "OVER (PARTITION BY e.sample_identifier)" not in src
