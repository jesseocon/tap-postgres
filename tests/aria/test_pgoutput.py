"""pgoutput logical replication: the decoder on crafted messages, and a real sync through a slot and
publication on the throwaway cluster (wal_level=logical)."""
import struct

import pytest

from tap_postgres.sync_strategies import pgoutput

REPL = 'aria_repl'


def _s(text):
    return text.encode() + b'\x00'


def _relation(relid, schema, table, cols):
    out = b'R' + struct.pack('!i', relid) + _s(schema) + _s(table) + b'd' + struct.pack('!h', len(cols))
    for name, oid, key in cols:
        out += bytes([1 if key else 0]) + _s(name) + struct.pack('!ii', oid, -1)
    return out


def _tuple(values):
    out = struct.pack('!h', len(values))
    for v in values:
        if v is None:
            out += b'n'
        elif v == 'TOAST':
            out += b'u'
        else:
            data = v.encode()
            out += b't' + struct.pack('!i', len(data)) + data
    return out


COLS = [('id', 23, True), ('total', 1700, False), ('paid', 16, False), ('note', 25, False), ('ratio', 701, False)]


def test_the_decoder_turns_messages_into_wal2json_shaped_changes():
    d = pgoutput.Decoder()
    assert d.feed(b'B' + b'\x00' * 20) is None
    assert d.feed(_relation(7, 'public', 'orders', COLS)) is None
    assert d.feed(b'I' + struct.pack('!i', 7) + b'N' + _tuple(['1', '10.50', 't', None, '0.5'])) == {
        'kind': 'insert', 'schema': 'public', 'table': 'orders',
        'columnnames': ['id', 'total', 'paid', 'note', 'ratio'], 'columnvalues': [1, '10.50', True, None, 0.5]}
    update = d.feed(b'U' + struct.pack('!i', 7) + b'N' + _tuple(['1', '12', 'f', 'TOAST', '1']))
    assert update['kind'] == 'update' and update['columnnames'] == ['id', 'total', 'paid', 'ratio']
    assert update['columnvalues'] == [1, '12', False, 1.0]
    old_key_update = d.feed(b'U' + struct.pack('!i', 7) + b'K' + _tuple(['1', None, None, None, None]) + b'N' + _tuple(['2', '1', 't', 'x', '2']))
    assert old_key_update['columnvalues'][0] == 2
    assert d.feed(b'D' + struct.pack('!i', 7) + b'K' + _tuple(['2', None, None, None, None])) == {
        'kind': 'delete', 'schema': 'public', 'table': 'orders', 'oldkeys': {'keynames': ['id'], 'keyvalues': [2]}}
    assert d.feed(b'C' + b'\x00' * 25) is None


def test_a_change_before_its_relation_is_an_error():
    with pytest.raises(pgoutput.PgOutputError):
        pgoutput.Decoder().feed(b'I' + struct.pack('!i', 9) + b'N' + _tuple(['1']))


@pytest.fixture
def logical(db):
    db.sql(f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{REPL}') THEN "
           f"CREATE ROLE {REPL} LOGIN REPLICATION PASSWORD 'y'; END IF; END $$")
    db.sql('CREATE TABLE public.orders (id int PRIMARY KEY, total numeric(10,2), paid boolean, note text)')
    db.sql("INSERT INTO public.orders VALUES (1, 10.50, true, 'first'), (2, 20, false, 'second')")
    db.sql(f'GRANT CONNECT ON DATABASE {db.name} TO {REPL}')
    db.sql(f'GRANT USAGE ON SCHEMA public TO {REPL}')
    db.sql(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {REPL}')
    db.sql('CREATE PUBLICATION aria_pub FOR TABLE public.orders')
    slot = f'aria_{db.name}'
    db.sql(f"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')")
    try:
        yield slot
    finally:
        db.sql(f"SELECT pg_drop_replication_slot('{slot}')")


def _config(slot):
    return {'user': REPL, 'password': 'y', 'default_replication_method': 'LOG_BASED', 'logical_plugin': 'pgoutput',
            'replication_slot': slot, 'publication_names': 'aria_pub', 'logical_poll_total_seconds': 3}


def test_pgoutput_sync_copies_then_streams_inserts_updates_and_deletes(db, logical):
    first = db.sync(**_config(logical))
    rows = sorted(first.records('public__orders'), key=lambda r: r['id'])
    assert [(r['id'], r['note']) for r in rows] == [(1, 'first'), (2, 'second')]
    assert db.state['bookmarks']['public__orders'].get('lsn')

    db.sql("INSERT INTO public.orders VALUES (3, 30, true, 'third')")
    db.sql("UPDATE public.orders SET total = 11, paid = false WHERE id = 1")
    db.sql('DELETE FROM public.orders WHERE id = 2')
    second = db.sync(**_config(logical))
    changes = second.records('public__orders')
    by_id = {}
    for r in changes:
        by_id.setdefault(r['id'], []).append(r)
    assert by_id[3][-1]['note'] == 'third' and by_id[3][-1]['paid'] is True and by_id[3][-1]['_sdc_deleted_at'] is None
    assert by_id[1][-1]['paid'] is False and float(by_id[1][-1]['total']) == 11
    assert by_id[2][-1]['_sdc_deleted_at'] is not None

    # Nothing new: at most the last change again (the slot is confirmed up to the previous run's
    # committed position, so the boundary change can be re-sent; the MERGE makes it harmless).
    third = db.sync(**_config(logical))
    assert all(r['id'] == 2 and r['_sdc_deleted_at'] for r in third.records('public__orders'))


def test_a_missing_named_slot_fails_plainly(db, logical):
    db.sync(**_config(logical))  # the first sync copies the tables; streaming starts with the next
    with pytest.raises(RuntimeError, match='Unable to find replication slot no_such_slot'):
        db.sync(**dict(_config(logical), replication_slot='no_such_slot'))
