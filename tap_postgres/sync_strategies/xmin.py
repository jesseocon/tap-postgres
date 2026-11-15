"""XMIN replication: an incremental sync that needs no replication key, no slot and no writes.

A row's xmin is the 32-bit id of the transaction that wrote that version of it, so a row that
was inserted or updated since the last read has an xmin at or after the last read's snapshot.
Snapshots give 64-bit ids (epoch << 32 | xid): pg_current_snapshot() on Postgres 13+,
txid_current_snapshot() before. Both are read-only and work on a hot standby, where they are the
primary's ids. txid_current() / pg_current_xact_id() are never called: they assign a
transaction id, a write on a primary and an error on a standby.

Per table (bookmarks, all in the stream's state):

- Initial load (no `xmin_snapshot` yet): every row, in chunks of `chunk_rows` ordered by the
  primary key, each chunk its own short READ ONLY transaction. `initial_load` holds the first
  chunk's snapshot xmin and the last key read, so a run cut short (time_budget_seconds) resumes
  where it stopped. When it ends, `xmin_snapshot` is that first snapshot xmin.
- Incremental: rows whose xmin is in [xmin_snapshot, snapshot xmax), walked by primary key in
  ranges of `xmin_chunk_keys` keys (xmin has no index). The new bookmark is the earliest chunk's
  snapshot xmin: every transaction below it had finished and was seen. A transaction still
  running then is read next time, so a row can be read twice, never missed.
- Re-sync: a bookmark that can't be compared with the snapshot (transaction ids moved half the
  32-bit circle since, or went backwards: another cluster) starts a new initial load. A replica
  a little behind the bookmark reads nothing this time.
- Deletes: a deleted row leaves no xmin. Every `key_scan_interval_hours` (or when `key_scan` is
  set) the table's primary keys are read and sent as records holding only the key columns and
  `_sdc_key_only: true`. A loader that sees them marks the rows it holds whose keys were not
  sent as deleted (aria-singer-taps: `_aria_deleted_at`); other loaders can ignore them.
- Tables and views without a primary key are read whole each run, in one READ ONLY transaction.

On a hot standby a read can be cancelled when replaying the primary's vacuum conflicts with it
(SQLSTATE 40001, after max_standby_streaming_delay). A chunk is buffered before it is sent, so
it is retried, at half its size, after a backoff; when retries run out the sync fails.
"""
import copy
import datetime
import random
import time

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras
import singer

from singer import utils
from singer import metrics

import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common

LOGGER = singer.get_logger('tap_postgres')

MOD = 1 << 32
HALF = 1 << 31
FIRST_NORMAL_XID = 3
# A snapshot this far behind the bookmark is a lagging replica, not another cluster.
REPLICA_LAG_TOLERANCE = 1 << 24
ROW_XMIN = 'xmin::text::bigint'

KEY_ONLY_COLUMN = '_sdc_key_only'
KEY_ONLY_SCHEMA = {'type': ['null', 'boolean']}

DEFAULT_CHUNK_ROWS = 10000
DEFAULT_XMIN_CHUNK_KEYS = 100000
DEFAULT_KEY_SCAN_CHUNK_KEYS = 100000
DEFAULT_KEY_SCAN_INTERVAL_HOURS = 24
DEFAULT_RETRIES = 4
MIN_CHUNK = 100

STATE_KEYS = {'xmin_snapshot', 'initial_load', 'key_scan_at', 'last_replication_method', 'version'}


class Resync(Exception):
    """The bookmark can't be compared with this snapshot: the table must be read again."""


class TimeBudgetExhausted(Exception):
    """time_budget_seconds ran out: the sync stops after the chunk it finished; the state says
    where to resume."""


# --- the xmin window ------------------------------------------------------------------------

def xmin_window(bookmark, snap_xmax):
    """SQL on the row's xmin for transactions in [bookmark, snap_xmax), and its parameters.

    Returns (None, ()) when nothing can have changed (or a replica is a little behind the
    bookmark). Raises Resync past wraparound range or when ids went backwards far.
    """
    if snap_xmax < bookmark:
        if bookmark - snap_xmax <= REPLICA_LAG_TOLERANCE:
            return None, ()
        raise Resync(f'transaction ids went backwards ({snap_xmax} < bookmark {bookmark}): another cluster')
    if snap_xmax - bookmark >= HALF:
        raise Resync(f'{snap_xmax - bookmark} transactions since the bookmark: past the xid wraparound range')
    if snap_xmax == bookmark:
        return None, ()
    low, high = bookmark % MOD, snap_xmax % MOD
    if low < high:
        return (f'({ROW_XMIN} >= %s AND {ROW_XMIN} < %s AND {ROW_XMIN} >= {FIRST_NORMAL_XID})', (low, high))
    return (f'(({ROW_XMIN} >= %s OR {ROW_XMIN} < %s) AND {ROW_XMIN} >= {FIRST_NORMAL_XID})', (low, high))


def in_window(xmin32, bookmark, snap_xmax):
    """Python mirror of xmin_window, for tests."""
    sql, params = xmin_window(bookmark, snap_xmax)
    if sql is None or xmin32 < FIRST_NORMAL_XID:
        return False
    low, high = params
    if low < high:
        return low <= xmin32 < high
    return xmin32 >= low or xmin32 < high


def snapshot_sql(server_version_num):
    if server_version_num >= 130000:
        return ('SELECT pg_snapshot_xmin(s)::text::bigint, pg_snapshot_xmax(s)::text::bigint '
                'FROM pg_current_snapshot() AS s')
    return 'SELECT txid_snapshot_xmin(s), txid_snapshot_xmax(s) FROM txid_current_snapshot() AS s'


# --- reads ----------------------------------------------------------------------------------

class Reader:
    """One connection; every read a short REPEATABLE READ, READ ONLY transaction that takes its
    snapshot first, so the rows and the snapshot's xmin/xmax agree."""

    def __init__(self, conn_config, retries=DEFAULT_RETRIES, sleep=time.sleep):
        self.conn_config = conn_config
        self.retries = retries
        self.sleep = sleep
        self.conn = None
        self.server_version_num = None
        self.hstore = None

    def connect(self):
        if self.conn is not None and not self.conn.closed:
            return self.conn
        if self.hstore is None:
            self.hstore = post_db.hstore_available(self.conn_config)
        self.conn = post_db.open_connection(self.conn_config)
        self.conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
                              readonly=True, autocommit=False)
        if self.hstore:
            psycopg2.extras.register_hstore(self.conn)
        with self.conn.cursor() as cur:
            cur.execute('SHOW server_version_num')
            self.server_version_num = int(cur.fetchone()[0])
        self.conn.rollback()
        return self.conn

    def close(self):
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
        self.conn = None

    def snapshot(self, cur):
        cur.execute(snapshot_sql(self.server_version_num))
        snap_xmin, snap_xmax = cur.fetchone()
        return int(snap_xmin), int(snap_xmax)

    def with_retries(self, what, limit, read):
        """read(limit) in a fresh transaction; on a standby conflict or a dropped connection,
        again with half the limit after a backoff."""
        attempt = 0
        while True:
            conn = self.connect()
            try:
                with conn.cursor() as cur:
                    result = read(cur, limit)
                conn.rollback()  # read only: nothing to commit; ends the snapshot
                return result, limit
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.QueryCanceled,
                    psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                attempt += 1
                self.close()
                if attempt > self.retries:
                    raise
                limit = max(MIN_CHUNK, limit // 2)
                wait = min(60.0, 2.0 ** attempt) * (1 + random.random() / 2)
                LOGGER.warning('%s: %s (%s); retrying in %.0fs with chunks of %s',
                               what, type(exc).__name__, getattr(exc, 'pgcode', None), wait, limit)
                self.sleep(wait)


def _key_sql(md_map, keys, qualified):
    # Ordered by the table's own columns, never an output column of the same name.
    order = ', '.join(f'{qualified}.{post_db.prepare_columns_sql(k)}' for k in keys)
    as_text = ', '.join(f'{qualified}.{post_db.prepare_columns_sql(k)}::text AS "__sdc_key_{i}"'
                        for i, k in enumerate(keys))
    types = [md_map[('properties', k)]['sql-datatype'] for k in keys]
    return order, as_text, types


def _compare(keys, types, op):
    left = ', '.join(post_db.prepare_columns_sql(k) for k in keys)
    right = ', '.join(f'%s::{t}' for t in types)
    if len(keys) == 1:
        return f'{left} {op} {right}'
    return f'({left}) {op} ({right})'


class Table:
    """What the reads of one stream need."""

    def __init__(self, stream, md_map, desired_columns):
        self.stream = stream
        self.md_map = md_map
        self.columns = desired_columns
        self.schema_name = md_map.get(()).get('schema-name')
        self.qualified = post_db.fully_qualified_table_name(self.schema_name, stream['table_name'])
        self.keys = list(md_map.get((), {}).get('table-key-properties') or [])
        self.is_view = bool(md_map.get((), {}).get('is-view'))
        self.selects = ', '.join(post_db.prepare_columns_for_select_sql(c, md_map) for c in desired_columns)
        if self.keys:
            self.order, self.key_text, self.key_types = _key_sql(md_map, self.keys, self.qualified)

    @property
    def tap_stream_id(self):
        return self.stream['tap_stream_id']

    def chunk(self, cur, reader, after, limit):
        """Up to `limit` rows after the key `after`: (snapshot, rows, last key, done)."""
        snap = reader.snapshot(cur)
        params = []
        where = ''
        if after is not None:
            where = 'WHERE ' + _compare(self.keys, self.key_types, '>')
            params.extend(after)
        cur.execute(f'SELECT {self.selects}, {self.key_text} FROM {self.qualified} {where} '
                    f'ORDER BY {self.order} LIMIT %s', (*params, limit))
        fetched = cur.fetchall()
        width = len(self.columns)
        last = list(fetched[-1][width:]) if fetched else after
        return snap, [r[:width] for r in fetched], last, len(fetched) < limit

    def bounded_chunk(self, cur, reader, after, limit, bookmark):
        """The rows changed since the bookmark among the next `limit` keys after `after`:
        (snapshot, rows, upper key or None at the end). The upper key is found first (a walk
        of the key's index), then that key range is read with the xmin filter."""
        snap = reader.snapshot(cur)
        params = []
        bound = []
        if after is not None:
            bound.append(_compare(self.keys, self.key_types, '>'))
            params.extend(after)
        where = f"WHERE {' AND '.join(bound)}" if bound else ''
        cur.execute(f'SELECT {self.key_text} FROM {self.qualified} {where} ORDER BY {self.order} '
                    f'OFFSET %s LIMIT 1', (*params, max(limit - 1, 0)))
        row = cur.fetchone()
        upper = list(row) if row else None
        if upper is not None:
            bound.append(_compare(self.keys, self.key_types, '<='))
            params.extend(upper)
        window, window_params = xmin_window(bookmark, snap[1])
        if window is None:
            return snap, [], upper
        bound.append(window)
        params.extend(window_params)
        cur.execute(f"SELECT {self.selects} FROM {self.qualified} WHERE {' AND '.join(bound)} "
                    f'ORDER BY {self.order}', params)
        return snap, cur.fetchall(), upper

    def keys_chunk(self, cur, after, limit):
        """The next `limit` primary keys: (key rows as values, key rows as text)."""
        params = []
        where = ''
        if after is not None:
            where = 'WHERE ' + _compare(self.keys, self.key_types, '>')
            params.extend(after)
        key_cols = ', '.join(post_db.prepare_columns_for_select_sql(k, self.md_map) for k in self.keys)
        cur.execute(f'SELECT {key_cols}, {self.key_text} FROM {self.qualified} {where} '
                    f'ORDER BY {self.order} LIMIT %s', (*params, limit))
        fetched = cur.fetchall()
        width = len(self.keys)
        return [r[:width] for r in fetched], (list(fetched[-1][width:]) if fetched else after)


# --- sync -----------------------------------------------------------------------------------

def schema_stream(stream, md_map):
    """The stream as its SCHEMA message describes it: with _sdc_key_only when keys are scanned."""
    keys = md_map.get((), {}).get('table-key-properties') or []
    if not keys or md_map.get((), {}).get('is-view'):
        return stream
    out = copy.deepcopy(stream)
    out['schema']['properties'][KEY_ONLY_COLUMN] = KEY_ONLY_SCHEMA
    return out


def _emit(table, rows, columns, time_extracted, counter):
    for row in rows:
        singer.write_message(post_db.selected_row_to_singer_message(
            table.stream, row, None, columns, time_extracted, table.md_map))
        counter.increment()


def _write_state(state):
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def _check_budget(deadline, what):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeBudgetExhausted(what)


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def key_scan_due(state, tap_stream_id, conn_config, now=None):
    if conn_config.get('key_scan'):
        return True
    hours = conn_config.get('key_scan_interval_hours')
    hours = DEFAULT_KEY_SCAN_INTERVAL_HOURS if hours is None else float(hours)
    if hours <= 0:
        return False
    last = singer.get_bookmark(state, tap_stream_id, 'key_scan_at')
    if not last:
        return True
    try:
        last_at = datetime.datetime.fromisoformat(str(last))
    except ValueError:
        return True
    return (now or _now()) - last_at >= datetime.timedelta(hours=hours)


def sync_table(conn_config, stream, state, desired_columns, md_map, deadline=None, reader=None):
    """One XMIN stream. Returns the new state; raises TimeBudgetExhausted when the budget ran
    out mid-table (the state then holds where to resume)."""
    table = Table(stream, md_map, desired_columns)
    tap_stream_id = table.tap_stream_id
    stream_state = state.get('bookmarks', {}).get(tap_stream_id, {})
    unknown = set(stream_state).difference(STATE_KEYS)
    if unknown:
        LOGGER.warning('%s: dropping state keys from another replication method: %s', tap_stream_id, sorted(unknown))
        for key in unknown:
            state['bookmarks'][tap_stream_id].pop(key, None)

    sync_common.send_schema_message(schema_stream(stream, md_map), [])
    reader = reader or Reader(conn_config, int(conn_config.get('standby_retries', DEFAULT_RETRIES)))
    try:
        if not table.keys or table.is_view:
            return _full_read(table, state, reader)
        if singer.get_bookmark(state, tap_stream_id, 'xmin_snapshot') is not None and \
                not singer.get_bookmark(state, tap_stream_id, 'initial_load'):
            try:
                state = _incremental(table, state, reader, conn_config)
            except Resync as exc:
                LOGGER.warning('%s: reading the table again: %s', tap_stream_id, exc)
                state = singer.write_bookmark(state, tap_stream_id, 'xmin_snapshot', None)
                state = singer.write_bookmark(state, tap_stream_id, 'initial_load', None)
        if singer.get_bookmark(state, tap_stream_id, 'xmin_snapshot') is None:
            state = _initial_load(table, state, reader, conn_config, deadline)
            # A first load reads every key, so a scan is only due when asked for (key_scan).
            if not conn_config.get('key_scan'):
                return state
        if key_scan_due(state, tap_stream_id, conn_config):
            if deadline is not None and time.monotonic() >= deadline:
                LOGGER.info('%s: key scan left for the next run (time budget used up)', tap_stream_id)
            else:
                state = _key_scan(table, state, reader, conn_config)
        return state
    finally:
        reader.close()


def _initial_load(table, state, reader, conn_config, deadline):
    tap_stream_id = table.tap_stream_id
    progress = singer.get_bookmark(state, tap_stream_id, 'initial_load') or {}
    after = progress.get('after')
    start_xmin = progress.get('xmin')
    limit = int(conn_config.get('chunk_rows') or DEFAULT_CHUNK_ROWS)
    LOGGER.info('%s: initial load %s', tap_stream_id, 'resumed' if after is not None else 'started')
    time_extracted = utils.now()
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            (snap, rows, after, done), limit = reader.with_retries(
                tap_stream_id, limit, lambda cur, n, a=after: table.chunk(cur, reader, a, n))
            if start_xmin is None:
                start_xmin = snap[0]
            _emit(table, rows, table.columns, time_extracted, counter)
            if done:
                break
            state = singer.write_bookmark(state, tap_stream_id, 'initial_load', {'xmin': start_xmin, 'after': after})
            _write_state(state)
            _check_budget(deadline, f'{tap_stream_id}: initial load')
    state = singer.write_bookmark(state, tap_stream_id, 'initial_load', None)
    state = singer.write_bookmark(state, tap_stream_id, 'xmin_snapshot', start_xmin)
    # The load read every key: the next key scan is due an interval from now.
    state = singer.write_bookmark(state, tap_stream_id, 'key_scan_at', _now().isoformat())
    LOGGER.info('%s: initial load done', tap_stream_id)
    return state


def _incremental(table, state, reader, conn_config):
    tap_stream_id = table.tap_stream_id
    bookmark = int(singer.get_bookmark(state, tap_stream_id, 'xmin_snapshot'))
    limit = int(conn_config.get('xmin_chunk_keys') or DEFAULT_XMIN_CHUNK_KEYS)
    after = None
    earliest = None
    time_extracted = utils.now()
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            (snap, rows, upper), limit = reader.with_retries(
                tap_stream_id, limit, lambda cur, n, a=after: table.bounded_chunk(cur, reader, a, n, bookmark))
            if snap[1] < bookmark:
                LOGGER.info('%s: the replica is behind the bookmark; nothing to read yet', tap_stream_id)
                return state
            earliest = snap[0] if earliest is None else min(earliest, snap[0])
            _emit(table, rows, table.columns, time_extracted, counter)
            if upper is None:
                break
            after = upper
    # Never later than any chunk's snapshot xmin: a transaction running during the read is read
    # again next time.
    state = singer.write_bookmark(state, tap_stream_id, 'xmin_snapshot', earliest)
    return state


def _key_scan(table, state, reader, conn_config):
    """Every primary key, as _sdc_key_only records. Never stopped part-way by the time budget:
    a loader takes the keys it got as the whole table, so a partial scan would mark live rows."""
    tap_stream_id = table.tap_stream_id
    limit = int(conn_config.get('key_scan_chunk_keys') or DEFAULT_KEY_SCAN_CHUNK_KEYS)
    started = _now()
    after = None
    time_extracted = utils.now()
    columns = [*table.keys, KEY_ONLY_COLUMN]
    md_map = dict(table.md_map)
    md_map[('properties', KEY_ONLY_COLUMN)] = {'sql-datatype': 'boolean'}
    keyed = copy.copy(table)
    keyed.md_map = md_map
    total = 0
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            (rows, after), limit = reader.with_retries(
                tap_stream_id, limit, lambda cur, n, a=after: table.keys_chunk(cur, a, n))
            _emit(keyed, [(*r, True) for r in rows], columns, time_extracted, counter)
            total += len(rows)
            if len(rows) < limit:
                break
    LOGGER.info('%s: key scan sent %s keys', tap_stream_id, total)
    return singer.write_bookmark(state, tap_stream_id, 'key_scan_at', started.isoformat())


def _full_read(table, state, reader):
    """No primary key (or a view): every row in one READ ONLY transaction. Retried only when it
    failed before the first row was sent."""
    tap_stream_id = table.tap_stream_id
    LOGGER.info('%s: no primary key; reading the whole table', tap_stream_id)
    time_extracted = utils.now()
    attempt = 0
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            sent = 0
            conn = reader.connect()
            try:
                with conn.cursor(name='pipelinewise_xmin_full') as cur:
                    cur.itersize = post_db.CURSOR_ITER_SIZE
                    cur.execute(f'SELECT {table.selects} FROM {table.qualified}')
                    for row in cur:
                        _emit(table, [row], table.columns, time_extracted, counter)
                        sent += 1
                conn.rollback()
                return state
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.QueryCanceled,
                    psycopg2.OperationalError) as exc:
                reader.close()
                attempt += 1
                if sent or attempt > reader.retries:
                    raise
                wait = min(60.0, 2.0 ** attempt)
                LOGGER.warning('%s: %s before the first row; retrying in %.0fs', tap_stream_id, type(exc).__name__, wait)
                reader.sleep(wait)
