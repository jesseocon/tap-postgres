"""XMIN replication, key scans, include/exclude, read-only sessions and standby retries."""
import random

import psycopg2
import psycopg2.errors
import pytest

from tap_postgres.sync_strategies import xmin
from tap_postgres.sync_strategies.xmin import HALF, MOD, Resync, in_window, xmin_window


# --- the window on the 32-bit circle ----------------------------------------------------------

def test_a_plain_range_takes_bookmark_to_xmax():
    sql, params = xmin_window(1000, 1500)
    assert params == (1000, 1500) and ' OR ' not in sql
    assert in_window(1000, 1000, 1500) and in_window(1499, 1000, 1500)
    assert not in_window(1500, 1000, 1500) and not in_window(999, 1000, 1500)


def test_the_range_wraps_past_two_to_the_32():
    bookmark, xmax = 3 * MOD - 10, 3 * MOD + 20
    sql, _ = xmin_window(bookmark, xmax)
    assert ' OR ' in sql
    assert in_window(MOD - 5, bookmark, xmax) and in_window(10, bookmark, xmax)
    assert not in_window(2, bookmark, xmax)  # frozen / bootstrap ids are never changes
    assert not in_window(25, bookmark, xmax) and not in_window(MOD - 11, bookmark, xmax)


def test_nothing_changed_or_a_lagging_replica_reads_nothing():
    assert xmin_window(500, 500) == (None, ())
    assert xmin_window(10_000, 9_990) == (None, ())


def test_wraparound_distance_and_another_cluster_resync():
    with pytest.raises(Resync):
        xmin_window(10, 10 + HALF)
    with pytest.raises(Resync):
        xmin_window(5 * MOD, 1_000)


def test_every_xid_in_range_is_taken_on_the_circle():
    rng = random.Random(7)
    for _ in range(2000):
        bookmark = rng.randrange(3, 6 * MOD)
        xmax = bookmark + rng.randrange(0, HALF)
        for full in (bookmark, xmax - 1, rng.randrange(bookmark, xmax + 1)):
            if full < xmax and full % MOD >= 3:
                assert in_window(full % MOD, bookmark, xmax), (bookmark, xmax, full)


# --- against a real Postgres ------------------------------------------------------------------

def _ids(rows):
    return sorted(r['id'] for r in rows)


def test_initial_load_resumes_across_a_time_budget_then_reads_only_changes(db):
    db.sql('CREATE TABLE items (id int PRIMARY KEY, name text, n numeric)')
    db.sql("INSERT INTO items SELECT g, 'item ' || g, g * 1.5 FROM generate_series(1, 2500) g")
    first = db.sync(chunk_rows=1000, time_budget_seconds=0.000001)
    assert len(first.records('public__items')) == 1000
    progress = first.state['bookmarks']['public__items']['initial_load']
    assert progress['after'] == ['1000'] and first.state['currently_syncing'] == 'public__items'

    db.sql("UPDATE items SET name = 'changed' WHERE id = 5")  # behind the cursor: read by the incremental
    rest = db.sync(chunk_rows=1000)
    assert _ids(rest.records('public__items')) == list(range(1001, 2501))
    bookmarks = rest.state['bookmarks']['public__items']
    assert bookmarks['xmin_snapshot'] and 'initial_load' not in bookmarks or bookmarks.get('initial_load') is None

    db.sql("UPDATE items SET name = 'again' WHERE id = 7")
    db.sql("INSERT INTO items VALUES (3000, 'new', 1)")
    changed = db.sync()
    assert _ids(changed.records('public__items')) == [5, 7, 3000]
    assert db.sync().records('public__items') == []


def test_composite_text_keys_chunk_correctly(db):
    db.sql('CREATE TABLE pairs (a text, b int, v text, PRIMARY KEY (a, b))')
    db.sql("INSERT INTO pairs SELECT chr(97 + g % 5) || ' x''y', g, 'v' FROM generate_series(1, 777) g")
    rows = db.sync(chunk_rows=50, xmin_chunk_keys=60).records('public__pairs')
    assert len({(r['a'], r['b']) for r in rows}) == 777
    db.sql("UPDATE pairs SET v = 'w' WHERE b IN (3, 400, 777)")
    assert sorted(r['b'] for r in db.sync(xmin_chunk_keys=60).records('public__pairs')) == [3, 400, 777]


def test_key_scan_sends_every_key_only_when_due(db):
    db.sql('CREATE TABLE items (id int PRIMARY KEY, name text)')
    db.sql("INSERT INTO items SELECT g, 'n' FROM generate_series(1, 30) g")
    first = db.sync(key_scan_interval_hours=24)
    assert first.records('public__items', key_only=True) == []  # the initial load read every key
    assert first.schema('public__items')['schema']['properties']['_sdc_key_only'] == {'type': ['null', 'boolean']}

    db.sql('DELETE FROM items WHERE id IN (3, 4)')
    assert db.sync(key_scan_interval_hours=24).records('public__items') == []
    scan = db.sync(key_scan=True, key_scan_chunk_keys=7)
    keys = scan.records('public__items', key_only=True)
    assert _ids(keys) == [i for i in range(1, 31) if i not in (3, 4)]
    assert set(keys[0]) == {'id', '_sdc_key_only'}
    assert scan.records('public__items', key_only=False) == []
    # interval 0: never, unless asked
    assert db.sync(key_scan_interval_hours=0).records('public__items') == []


def test_a_bookmark_past_wraparound_reads_the_table_again(db):
    db.sql('CREATE TABLE items (id int PRIMARY KEY)')
    db.sql('INSERT INTO items SELECT generate_series(1, 10)')
    db.sync()
    state = db.state
    state['bookmarks']['public__items']['xmin_snapshot'] = 5 * MOD  # another cluster's ids
    again = db.sync(state=state)
    assert _ids(again.records('public__items')) == list(range(1, 11))
    assert 'reading the table again' in again.stderr


def test_tables_without_a_primary_key_and_views_are_read_whole_each_run(db):
    db.sql('CREATE TABLE log (msg text)')
    db.sql("INSERT INTO log VALUES ('a'), ('b')")
    db.sql('CREATE VIEW log_view AS SELECT msg FROM log')
    db.sql('GRANT SELECT ON log_view TO aria_sync')
    for _ in range(2):
        result = db.sync()
        assert sorted(r['msg'] for r in result.records('public__log')) == ['a', 'b']
        assert len(result.records('public__log_view')) == 2
        assert '_sdc_key_only' not in result.schema('public__log')['schema']['properties']
        assert result.schema('public__log')['key_properties'] == []


def test_include_and_exclude_tables_and_new_tables_sync(db):
    for name in ('users', 'oauth_tokens', 'mcp_api_keys', 'orders'):
        db.sql(f'CREATE TABLE {name} (id int PRIMARY KEY)')
    names = {s['tap_stream_id'] for s in db.discover(exclude_tables=['oauth_*', 'public.mcp_api_keys'])['streams']}
    assert names == {'public__users', 'public__orders'}
    names = {s['tap_stream_id'] for s in db.discover(include_tables='users,orders,oauth_*', exclude_tables='oauth_tokens')['streams']}
    assert names == {'public__users', 'public__orders'}
    db.sql('CREATE TABLE later (id int PRIMARY KEY)')
    assert 'public__later' in {s['tap_stream_id'] for s in db.discover(exclude_tables=['oauth_*'])['streams']}
    db.sql('CREATE SCHEMA other')
    db.sql('CREATE TABLE other.users (id int PRIMARY KEY)')
    db.sql('GRANT USAGE ON SCHEMA other TO aria_sync')
    db.sql('GRANT SELECT ON other.users TO aria_sync')
    names = {s['tap_stream_id'] for s in db.discover(filter_schemas='public,other', exclude_schemas='pub*')['streams']}
    assert names == {'other__users'}
    # without a separator the names stay the tables' own (Hotglue's behaviour)
    names = {s['tap_stream_id'] for s in db.discover(stream_name_separator=None)['streams']}
    assert 'users' in names


def test_partitions_are_read_through_their_parent_when_asked(db):
    db.sql('CREATE TABLE events (id int, day date, PRIMARY KEY (id, day)) PARTITION BY RANGE (day)')
    db.sql("CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
    db.sql('GRANT SELECT ON events, events_2026 TO aria_sync')
    db.sql("INSERT INTO events VALUES (1, '2026-03-01'), (2, '2026-04-01')")
    assert {s['tap_stream_id'] for s in db.discover(exclude_partitions=True)['streams']} == {'public__events'}
    rows = db.sync(exclude_partitions=True).records('public__events')
    assert _ids(rows) == [1, 2]


def test_sessions_are_read_only_even_for_a_role_that_can_write(db, cluster):
    from tap_postgres import db as post_db

    db.sql('CREATE TABLE items (id int PRIMARY KEY)')
    config = dict(cluster, dbname=db.name, password='', use_secondary=False)  # the superuser
    conn = post_db.open_connection(config)
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with conn.cursor() as cur:
            cur.execute('INSERT INTO items VALUES (1)')
    conn.rollback()
    conn.autocommit = True  # even outside psycopg2's BEGIN READ ONLY, the session default holds
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        with conn.cursor() as cur:
            cur.execute('INSERT INTO items VALUES (1)')
    conn.close()
    assert db.sql('SELECT count(*) FROM items') == [(0,)]


def test_a_cancelled_chunk_is_retried_smaller(db, cluster, monkeypatch, capsys):
    db.sql('CREATE TABLE items (id int PRIMARY KEY, name text)')
    db.sql("INSERT INTO items SELECT g, 'n' FROM generate_series(1, 300) g")
    catalog = db.discover()
    stream = catalog['streams'][0]
    from singer import metadata
    md_map = metadata.to_map(stream['metadata'])
    config = dict(cluster, dbname=db.name, user='aria_sync', password='x', use_secondary=False)

    real_chunk = xmin.Table.chunk
    calls = []

    def flaky(self, cur, reader, after, limit):
        calls.append(limit)
        if len(calls) <= 2:
            raise psycopg2.errors.SerializationFailure('canceling statement due to conflict with recovery')
        return real_chunk(self, cur, reader, after, limit)

    monkeypatch.setattr(xmin.Table, 'chunk', flaky)
    reader = xmin.Reader(config, retries=4, sleep=lambda _s: None)
    state = xmin.sync_table(dict(config, chunk_rows=1000), stream, {}, ['id', 'name'], md_map, reader=reader)
    assert calls[:3] == [1000, 500, 250]
    assert state['bookmarks'][stream['tap_stream_id']]['xmin_snapshot']
    out = capsys.readouterr().out
    assert out.count('"type": "RECORD"') + out.count('"type":"RECORD"') == 300

    calls.clear()
    monkeypatch.setattr(xmin.Table, 'chunk', lambda *a: (_ for _ in ()).throw(
        psycopg2.errors.SerializationFailure('conflict')))
    with pytest.raises(psycopg2.errors.SerializationFailure):
        xmin.sync_table(dict(config, chunk_rows=1000), stream, {}, ['id', 'name'], md_map,
                        reader=xmin.Reader(config, retries=2, sleep=lambda _s: None))


def test_types_come_through(db):
    db.sql("""CREATE TABLE t (id uuid PRIMARY KEY, j jsonb, a int[], ts timestamptz, d date, n numeric(12,2),
              b boolean, e text)""")
    db.sql("""INSERT INTO t VALUES ('8a1f6c2e-0000-4000-8000-000000000001', '{"x": [1, 2]}', '{1,2}',
              '2026-11-14 10:00:00+00', '2026-11-14', 12.34, true, 'é')""")
    row = db.sync().records('public__t')[0]
    assert row['j'] == {'x': [1, 2]} and row['a'] == [1, 2] and row['n'] == 12.34 and row['b'] is True
    assert row['e'] == 'é', row
    from datetime import datetime, timezone
    assert datetime.fromisoformat(row['ts']) == datetime(2026, 11, 14, 10, tzinfo=timezone.utc)
    db.sql("UPDATE t SET b = false")
    assert db.sync().records('public__t')[0]['b'] is False


def test_a_forced_key_scan_follows_a_first_load(db):
    """full_table use: every run starts without bookmarks and asks for a key scan."""
    db.sql('CREATE TABLE items (id int PRIMARY KEY)')
    db.sql('INSERT INTO items SELECT generate_series(1, 5)')
    result = db.sync(state={}, key_scan=True)
    assert _ids(result.records('public__items', key_only=False)) == [1, 2, 3, 4, 5]
    assert _ids(result.records('public__items', key_only=True)) == [1, 2, 3, 4, 5]
    # the full rows come first, then the keys
    kinds = [bool(m['record'].get('_sdc_key_only')) for m in result.messages if m['type'] == 'RECORD']
    assert kinds == [False] * 5 + [True] * 5
